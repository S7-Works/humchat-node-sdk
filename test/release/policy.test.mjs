import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { validateMetadata, validatePackFiles, requiredFiles, inspectText, validateDocLinks, assertSourceStable } from '../../scripts/release-policy.mjs';

const pkg = JSON.parse(await readFile(new URL('../../package.json', import.meta.url), 'utf8'));
const lock = JSON.parse(await readFile(new URL('../../package-lock.json', import.meta.url), 'utf8'));
const files = requiredFiles.map(path => ({ path, size: 12 }));

test('current private package and pinned lock satisfy local release policy', () => validateMetadata(pkg, lock));
test('accidental public/stable metadata or lifecycle scripts fail closed', () => {
  for (const change of [{ private: false }, { version: '1.0.0' }, { license: 'MIT' }, { publishConfig: { access: 'public' } },
    { scripts: { ...pkg.scripts, install: 'node install.js' } }, { scripts: { ...pkg.scripts, prepublishOnly: 'node publish.js' } }]) {
    assert.throws(() => validateMetadata({ ...pkg, ...change }, lock));
  }
});
test('lock mismatch and unpinned dependency cannot pass', () => {
  assert.throws(() => validateMetadata(pkg, { ...lock, version: '0.1.0-alpha.999' }));
  const changed = structuredClone(pkg); const changedLock = structuredClone(lock);
  changed.dependencies.ws = '^8.0.0'; changedLock.packages[''].dependencies.ws = '^8.0.0';
  assert.throws(() => validateMetadata(changed, changedLock));
});
test('allowlist rejects stale dist outputs, source maps, credentials and development files', () => {
  for (const path of ['dist/old.js', 'dist/index.js.map', '.env', 'examples/.env.local', 'src/config.ts', 'test/private.json', 'scripts/build.mjs', 'docs/private.txt']) {
    assert.throws(() => validatePackFiles([...files, { path, size: 1 }]));
  }
  assert.equal(validatePackFiles(files).size, requiredFiles.length);
});
test('paths must be canonical, unique, relative and bounded', () => {
  for (const path of ['../README.md', '/README.md', 'docs/../README.md', 'docs\\API.md', 'README.md']) {
    assert.throws(() => validatePackFiles([...files, { path, size: 1 }]));
  }
  assert.throws(() => validatePackFiles(files.map((x, i) => i === 0 ? { ...x, size: 3 * 1024 * 1024 } : x)));
  assert.throws(() => validatePackFiles(files.slice(1)));
});
test('credential marker scan rejects signing keys and tokens without echoing the value', () => {
  for (const text of ['bb_live_sk_' + 'a'.repeat(32), '-----BEGIN PRIVATE KEY-----', 'npm_' + 'x'.repeat(36),
    'eyJ' + 'a'.repeat(30) + '.' + 'b'.repeat(30) + '.' + 'c'.repeat(30), '//# sourceMappingURL=old.js.map']) {
    assert.throws(() => inspectText('README.md', text), error => !error.message.includes(text));
  }
  inspectText('README.md', 'Read the token from process.env.HUMCHAT_TOKEN; never commit credentials.');
});
test('installed docs links cannot depend on repository-only files', () => {
  const paths = new Set(requiredFiles);
  validateDocLinks('docs/API.md', '[README](../README.md) [Auth](AUTHENTICATION.md#tokens) [npm](https://docs.npmjs.com/)', paths);
  assert.throws(() => validateDocLinks('README.md', '[report](../../docs/HUMCHAT_NODE_SDK_PLAN.md)', paths));
  assert.throws(() => validateDocLinks('README.md', '[missing](docs/MISSING.md)', paths));
});
test('changes during validation invalidate the release bundle', () => {
  const before = [{ path: 'README.md', sha256: 'old' }];
  assertSourceStable(before, structuredClone(before));
  assert.throws(() => assertSourceStable(before, [{ path: 'README.md', sha256: 'new' }]));
  assert.throws(() => assertSourceStable(before, [...before, { path: 'examples/new.mjs', sha256: 'new' }]));
});
