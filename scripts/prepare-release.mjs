#!/usr/bin/env node
/** Build and verify a local private alpha tarball. No publish, login, tag or remote mutation. */
import { spawn } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import { lstat, mkdir, mkdtemp, readFile, readdir, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { validateMetadata, validatePackFiles, inspectText, validateDocLinks, assertSourceStable } from './release-policy.mjs';

if (process.argv.length !== 2) throw new Error('release:prepare takes no arguments and cannot publish');
const root = fileURLToPath(new URL('../', import.meta.url));
const workspace = root;
const npm = process.env.npm_execpath ?? join(dirname(process.execPath), 'node_modules/npm/bin/npm-cli.js');
const artifacts = join(workspace, 'target', 'release');
await mkdir(artifacts, { recursive: true });
const output = await mkdtemp(join(artifacts, 'release-'));
await mkdir(join(output, 'logs'));
const report = { schema: 1, startedAt: new Date().toISOString(), passed: false,
  node: process.version, platform: process.platform, arch: process.arch, checks: [],
  publication: { enabled: false, distribution: 'private local tarball',
    openGates: ['M1 verified machine authentication', 'M5 staging and agreed 24-hour pilot',
      'npm scope ownership and registry/access decision', 'license and support/security ownership', 'external-team acceptance'] } };
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const sourceRoots = ['src', 'scripts', 'test', 'examples', 'docs', 'contract'];
async function builtManifest() {
  return Promise.all(['index.js', 'index.cjs', 'index.d.ts', 'index.d.cts'].map(async name => {
    const path = 'dist/' + name;
    const bytes = await readFile(join(root, path));
    return { path, bytes: bytes.length, sha256: hash(bytes) };
  }));
}

async function sourceManifest() {
  const paths = ['package.json', 'package-lock.json', 'tsconfig.json', 'README.md', 'CHANGELOG.md', 'LICENSE'];
  async function collect(directory) {
    for (const item of await readdir(join(root, directory), { withFileTypes: true })) {
      const path = directory + '/' + item.name;
      if (item.isSymbolicLink()) throw new Error('Release source symlink is unsupported: ' + path);
      if (item.isDirectory()) await collect(path);
      else if (item.isFile()) paths.push(path);
      else throw new Error('Release source must be a regular file: ' + path);
    }
  }
  for (const directory of sourceRoots) await collect(directory);
  const records = [];
  for (const path of paths.sort()) {
    if (!(await lstat(join(root, path))).isFile()) throw new Error('Release source must be a regular file');
    const bytes = await readFile(join(root, path));
    records.push({ path, bytes: bytes.length, sha256: hash(bytes) });
  }
  return records;
}

async function command(name, program, args, extraEnv = {}, cwd = root) {
  console.log('Validating: ' + name);
  const logPath = 'logs/' + name + '.log';
  const log = createWriteStream(join(output, logPath));
  let stdout = '', code = null, failure;
  let timedOut = false;
  const started = performance.now();
  const child = spawn(program, args, { cwd, env: { ...process.env, ...extraEnv }, windowsHide: true,
    detached: process.platform !== 'win32',
    stdio: ['ignore', 'pipe', 'pipe'] });
  let finished = false, deadline, escalation, giveUp;
  child.stdout.on('data', data => { if (!finished) { stdout += data.toString(); log.write(data); } });
  child.stderr.on('data', data => { if (!finished) log.write(data); });
  function stopTree(force) {
    if (!child.pid) return;
    if (process.platform === 'win32') {
      // Only this command's process tree; no shell or process-name matching.
      const killer = spawn('taskkill.exe', ['/PID', String(child.pid), '/T', '/F'], { windowsHide: true, stdio: 'ignore' });
      killer.on('error', () => { child.kill('SIGKILL'); });
      killer.unref();
    } else {
      try { process.kill(-child.pid, force ? 'SIGKILL' : 'SIGTERM'); } catch {}
    }
  }
  try {
    code = await new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('close', resolve);
      deadline = setTimeout(() => {
        timedOut = true;
        stopTree(false);
        escalation = setTimeout(() => stopTree(true), 5_000);
        giveUp = setTimeout(() => {
          child.stdout.destroy(); child.stderr.destroy(); child.unref();
          reject(new Error('Validation process tree did not close after timeout'));
        }, 10_000);
      }, 15 * 60_000);
    });
  } catch (error) {
    failure = error;
  } finally {
    finished = true;
    clearTimeout(deadline); clearTimeout(escalation); clearTimeout(giveUp);
    await new Promise(resolve => log.end(resolve));
  }
  report.checks.push({ name, log: logPath, exitCode: code, timedOut, durationMs: Math.round(performance.now() - started) });
  if (failure || timedOut || code !== 0) throw new Error('Validation failed: ' + name + '; see ' + logPath);
  return stdout;
}

try {
  report.source = await sourceManifest();
  const pkg = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
  const lock = JSON.parse(await readFile(join(root, 'package-lock.json'), 'utf8'));
  validateMetadata(pkg, lock);
  report.package = pkg.name;
  report.version = pkg.version;
  report.gitHead = (await command('git-head', 'git', ['rev-parse', 'HEAD'], {}, workspace)).trim();
  report.sdkWorkingTree = (await command('git-status', 'git', ['status', '--porcelain', '--', '.'], {}, workspace)).trim();
  report.npm = (await command('npm-version', process.execPath, [npm, '--version'])).trim();
  await command('build', process.execPath, [npm, 'run', 'build']);
  report.builtOutputs = await builtManifest();
  for (const script of ['test:unit', 'test:integration', 'test:release']) {
    await command(script.replace(':', '-'), process.execPath, [npm, 'run', script]);
  }
  const packed = JSON.parse(await command('pack', process.execPath, [npm, 'pack', '--json', '--ignore-scripts', '--pack-destination', output]));
  if (packed.length !== 1 || packed[0].name !== pkg.name || packed[0].version !== pkg.version ||
      packed[0].filename !== 'humchat-node-' + pkg.version + '.tgz') throw new Error('Unexpected npm pack result');
  const item = packed[0];
  assertSourceStable(report.builtOutputs, await builtManifest());
  const paths = validatePackFiles(item.files);
  const files = [];
  for (const file of item.files) {
    // The pack policy admits only direct children of these directories.
    const parent = dirname(join(root, file.path));
    if (parent !== root && (await lstat(parent)).isSymbolicLink()) throw new Error('Packed directory symlink is unsupported');
    const stats = await lstat(join(root, file.path));
    if (!stats.isFile() || stats.size !== file.size) throw new Error('Packed source file changed or is not regular');
    const bytes = await readFile(join(root, file.path));
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    inspectText(file.path, text);
    if (file.path.endsWith('.md')) validateDocLinks(file.path, text, paths);
    files.push({ path: file.path, bytes: bytes.length, sha256: hash(bytes) });
  }
  for (const built of report.builtOutputs) {
    const packedFile = files.find(file => file.path === built.path);
    if (JSON.stringify(packedFile) !== JSON.stringify(built)) throw new Error('Packed build differs from tested outputs');
  }
  const tarball = join(output, item.filename);
  const bytes = await readFile(tarball);
  if ('sha512-' + createHash('sha512').update(bytes).digest('base64') !== item.integrity) throw new Error('npm pack integrity does not match the archive');
  report.artifact = { filename: item.filename, bytes: bytes.length, unpackedBytes: item.unpackedSize,
    sha256: hash(bytes), integrity: item.integrity, files };
  await command('test-package', process.execPath, [npm, 'run', 'test:package'], {
    HUMCHAT_SDK_PACKAGE_TARBALL: tarball, HUMCHAT_SDK_ARTIFACTS_DIR: join(output, 'consumers'),
  });
  const consumer = JSON.parse(await readFile(join(output, 'consumers', 'consumer-result.json'), 'utf8'));
  if (consumer.passed !== true || consumer.tarball?.sha256 !== report.artifact.sha256 || consumer.installedVersion !== pkg.version) {
    throw new Error('Consumer evidence does not match the validated archive');
  }
  const byPath = entries => [...entries].sort((a, b) => a.path.localeCompare(b.path));
  assertSourceStable(byPath(report.artifact.files), byPath(consumer.files));
  report.consumerEvidence = 'consumers/consumer-result.json';
  report.consumer = { runtime: consumer.runtime, examples: consumer.examples.map(({ name, passed, exitCode }) => ({ name, passed, exitCode })),
    quickstartSnippets: consumer.quickstartSnippets };
  if (hash(await readFile(tarball)) !== report.artifact.sha256) throw new Error('Tarball changed during validation');
  // The consumer verifier additionally compares every archive payload with its source
  // and installed bytes. Keep the build frozen across that complete validation window.
  assertSourceStable(report.builtOutputs, await builtManifest());
  assertSourceStable(report.source, await sourceManifest());
  await writeFile(join(output, 'SHA256SUMS'), report.artifact.sha256 + '  ' + item.filename + '\n');
  report.passed = true;
} catch (error) {
  report.failure = error instanceof Error ? error.message : 'Release preparation failed';
  process.exitCode = 1;
} finally {
  report.finishedAt = new Date().toISOString();
  await writeFile(join(output, 'release-manifest.json'), JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify({ passed: report.passed, package: report.package, version: report.version,
    output, publicationEnabled: false, ...(report.failure ? { failure: report.failure } : {}) }));
}
