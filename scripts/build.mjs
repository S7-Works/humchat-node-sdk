import { build } from 'esbuild';
import { generateDtsBundle } from 'dts-bundle-generator';
import { mkdir, readdir, unlink, writeFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';

const root = fileURLToPath(new URL('../', import.meta.url));
const dist = join(root, 'dist');
const checked = spawnSync(process.execPath, [join(root, 'node_modules/typescript/bin/tsc'), '--noEmit', '-p', join(root, 'tsconfig.json')],
  { cwd: root, stdio: 'inherit', windowsHide: true });
if (checked.error) throw checked.error;
if (checked.status !== 0) process.exit(checked.status ?? 1);
await mkdir(dist, { recursive: true });
// Delete only known outputs directly inside this package's dist directory.
const outputs = new Set(['index.js', 'index.cjs', 'index.d.ts', 'index.d.cts']);
for (const file of await readdir(dist)) if (outputs.has(file)) await unlink(join(dist, file));
await Promise.all(['esm', 'cjs'].map(format => build({
  absWorkingDir: root, entryPoints: ['src/index.ts'], outfile: join(dist, format === 'esm' ? 'index.js' : 'index.cjs'),
  bundle: true, platform: 'node', target: 'node22', format, packages: 'external',
  external: ['ulid', 'zod'], sourcemap: false, legalComments: 'none', logLevel: 'info',
})));
const [declaration] = generateDtsBundle([{ filePath: join(root, 'src/index.ts'), output: { noBanner: true, exportReferencedTypes: false } }],
  { preferredConfigPath: join(root, 'tsconfig.json') });
if (!declaration) throw new Error('Declaration bundle was empty');
// A self-contained bundle has no relative references whose module kind could differ.
if (/\b(?:from\s*|import\s*\()\s*['"]\.\.?\//.test(declaration)) throw new Error('Declaration bundle unexpectedly retains relative imports');
await Promise.all(['index.d.ts', 'index.d.cts'].map(name => writeFile(join(dist, name), declaration)));
console.log('Built ESM, CJS and matching self-contained declarations.');
