// Local alpha distribution policy. This module never contacts a registry.
import { posix } from 'node:path';

export const requiredFiles = [
  'package.json', 'README.md', 'CHANGELOG.md', 'LICENSE',
  'dist/index.js', 'dist/index.cjs', 'dist/index.d.ts', 'dist/index.d.cts',
  'docs/AUTHENTICATION.md', 'docs/RELIABILITY.md', 'docs/API.md', 'docs/MIGRATION.md',
  'docs/TROUBLESHOOTING.md', 'docs/RELEASING.md', 'docs/SUPPORT.md', 'docs/MULTIPLEX.md',
  'examples/README.md', 'examples/multiplex-worker.mjs',
  'docs/WEBHOOKS.md', 'examples/webhook-receiver.mjs', 'examples/webhook-inbox.mjs',
  'docs/DIRECT_MESSAGES.md', 'examples/dm-responder.mjs',
];

export function validateMetadata(pkg, lock) {
  if (pkg.name !== '@humchat/node') throw new Error('Package name requires a reviewed policy update');
  if (!/^0\.1\.0-alpha\.[0-9]+$/.test(pkg.version)) throw new Error('Only private alpha versions are permitted by this release policy');
  if (pkg.private !== true || pkg.license !== 'UNLICENSED') throw new Error('Private alpha/license policy changed; review release decisions first');
  if (pkg.publishConfig || pkg.bin) throw new Error('Unexpected publication or executable metadata');
  for (const hook of ['preinstall', 'install', 'postinstall', 'prepare', 'prepublish', 'prepublishOnly', 'publish', 'postpublish']) {
    if (pkg.scripts?.[hook]) throw new Error('Unexpected install/publication lifecycle hook: ' + hook);
  }
  if (pkg.engines?.node !== '>=22') throw new Error('Node compatibility policy changed');
  if (lock.name !== pkg.name || lock.version !== pkg.version || lock.packages?.['']?.version !== pkg.version ||
      lock.packages?.['']?.name !== pkg.name) throw new Error('Package and lock identity differ');
  for (const kind of ['dependencies', 'devDependencies']) {
    if (JSON.stringify(pkg[kind]) !== JSON.stringify(lock.packages[''][kind])) throw new Error('Package and lock dependencies differ');
    for (const version of Object.values(pkg[kind] ?? {})) {
      if (!/^\d+\.\d+\.\d+(?:-[a-zA-Z0-9.-]+)?$/.test(version)) throw new Error('Release dependencies must be exact versions');
    }
  }
}

export function validatePackFiles(files) {
  const seen = new Set();
  for (const file of files) {
    const path = file.path;
    if (typeof path !== 'string' || path !== posix.normalize(path) || path.startsWith('/') || path.includes('\\') ||
        path.split('/').some(part => part === '..' || part.startsWith('.')) || seen.has(path)) {
      throw new Error('Unsafe or duplicate packed path');
    }
    if (/^docs\/SDK_KEY_AUTH_(?:CONTRACT|PHASE[0-9]+_REPORT)\.md$/.test(path)) {
      throw new Error('Internal documentation cannot be included in the package');
    }
    if (!/^(?:package\.json|README\.md|CHANGELOG\.md|LICENSE|dist\/index\.(?:js|cjs|d\.ts|d\.cts)|docs\/[A-Z][A-Z0-9_]*\.md|examples\/(?:README\.md|[a-z][a-z0-9-]*\.(?:mjs|cjs)))$/.test(path)) {
      throw new Error('Unexpected packed file: ' + path);
    }
    if (!Number.isSafeInteger(file.size) || file.size < 0 || file.size > 2 * 1024 * 1024) throw new Error('Invalid packed file size');
    seen.add(path);
  }
  for (const path of requiredFiles) if (!seen.has(path)) throw new Error('Missing required package file: ' + path);
  return seen;
}

export function inspectText(path, text) {
  // Known credential shapes and signing-key material, not a general secret detector.
  // HumChat currently generates 43 base64url characters after hsk_. A 32-character
  // floor also catches nearby opaque formats without rejecting the documented prefix.
  if (/bb_live_(?:sk|cid)_[a-f0-9]{16,}/i.test(text) ||
      /hsk_[A-Za-z0-9_-]{32,}/.test(text) ||
      /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/.test(text) ||
      /eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}/.test(text) ||
      /npm_[a-zA-Z0-9]{30,}/.test(text)) throw new Error('Credential-shaped content detected in ' + path);
  if (/sourceMappingURL\s*=/.test(text)) throw new Error('Unexpected source map reference in ' + path);
}

export function validateDocLinks(path, text, packedPaths) {
  for (const match of text.matchAll(/\]\(([^\s)]+)(?:\s+"[^"]*")?\)/g)) {
    let target = match[1].replace(/^<|>$/g, '');
    if (/^(?:https?:\/\/|mailto:|#)/i.test(target)) continue;
    target = decodeURIComponent(target.split('#')[0]);
    if (!target) continue;
    const resolved = posix.normalize(posix.join(posix.dirname(path), target));
    if (!packedPaths.has(resolved)) throw new Error('Package documentation links outside its shipped files: ' + path + ' -> ' + target);
  }
}

export function assertSourceStable(before, after) {
  if (JSON.stringify(before) !== JSON.stringify(after)) throw new Error('Release sources changed during validation; prepare a new bundle');
}
