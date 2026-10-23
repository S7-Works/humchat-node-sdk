# Release preparation and publication

The current package version is `0.1.0-alpha.8`, a review preview with delegated member login through HumChat. Source is public in [S7-Works/humchat-node-sdk](https://github.com/S7-Works/humchat-node-sdk); no npm version is published. The package remains `private: true` and `UNLICENSED`. Production deployment validation and a sustained pilot remain open. A passing local artifact manifest does not establish production readiness or grant an open-source license.

## Prepare a local artifact

From the repository root with Node 22 or 24:

```sh
npm ci
npm run release:prepare
```

The command accepts no arguments. It builds ESM/CommonJS/declarations, runs unit and controlled-peer tests, validates the release policy, packs once, and installs that exact archive into independent ESM/CommonJS/TypeScript and example consumers. Package paths, shipped documentation links, known credential markers, checksums and source changes during the run are checked. The credential scan is deliberately limited; it does not replace review.

The command prints a unique `target/release/release-*` directory containing:

| Output | Purpose |
| --- | --- |
| `humchat-node-0.1.0-alpha.8.tgz` | Installable package with compiled outputs, documentation and examples. |
| `SHA256SUMS` | Archive hash for transfer verification. |
| `release-manifest.json` | Runtime, package identity, packed file/source hashes, check results and open publication gates. `passed: true` means local validation passed. |
| `logs/` and `consumers/` | Local validation evidence; these are not part of the npm tarball. |

On failure, the manifest remains with `passed: false`; do not distribute its tarball. Every run uses a fresh directory. A dirty checkout is allowed for local review and recorded; its source hashes identify the inputs. It is not a clean-checkout release attestation. CI uses a fresh checkout and `npm ci`. The release manifest is evidence about validation, not cryptographic publisher identity or npm provenance.

To verify a supplied archive, compare its SHA-256 with `SHA256SUMS`, then install it with `npm install /path/to/humchat-node-0.1.0-alpha.8.tgz`. Run the [examples](../examples/README.md) against an approved local or pilot environment using native `auth: { sdkKey, actAs }` or caller-managed `tokenProvider` authentication; configure exactly one mode. Do not use `npm install @humchat/node` until an actual registry release exists.

## Repository workflow

The repository's `.github/workflows/ci.yml` runs package checks on Node 22/24 across Ubuntu, Windows and macOS for pushes and pull requests. The manual `.github/workflows/release.yml` prepares and uploads a validated tarball, checksum and manifest. Neither workflow publishes to npm or uses production credentials. Check the [Actions results](https://github.com/S7-Works/humchat-node-sdk/actions) for actual results; workflow configuration alone is not passing evidence. Verify an artifact's archive SHA-256 against its accompanying checksum before using it.

Validation against a real HumChat deployment and an independent team's pilot remain separate gates. This repository does not build or start a HumChat server. The explicit [live message runner](SDK_KEY_MESSAGE_ACCEPTANCE.md) requires an authorized sender/recipient pair; production send/reply and app acceptance remain pending. Package checks use local synthetic peers and do not certify server capacity or HumChat app display.

## Decisions required before npm publication

| Decision / evidence | Current state |
| --- | --- |
| Package scope and registry ownership | S7 Works is the intended company npm owner. `@humchat/node` remains provisional; the exact company scope, availability and maintainer permissions are unverified. |
| Public versus restricted npm access | Unassigned. The GitHub repository is public; `private: true` still prevents npm publication. |
| License | `UNLICENSED`; owner decision pending. Public source visibility does not grant an open-source license. |
| Support/security owner | Public issues accept nonsensitive questions; dedicated security contact and support commitments remain unassigned. See [support](SUPPORT.md). |
| Production member messaging | Run the designated two-member DM/reply test and verify both messages in the HumChat app. This acceptance remains pending. |
| Delegated administrative boundary | Verify that the target server denies delegated sessions on administrative routes while allowing permitted messaging. SDK configuration does not establish server authorization. |
| Socket expiry recovery | Verify proactive renewal and any advertised `TOKEN_EXPIRED` behavior on the target deployment. An ambiguous revocation remains terminal. |
| Staging and sustained pilot | Required before release-candidate or stable promotion, with agreed capacity and failure acceptance. |
| Independent team acceptance | Team installs a versioned archive and runs the documented worker without SDK-team intervention; not yet observed. |

The implemented authentication contract is HumChat-only `auth: { sdkKey, actAs }`. See the [login guide](SDK_KEY_LOGIN_GUIDE.md) for authentication and renewal, and the [message acceptance procedure](SDK_KEY_MESSAGE_ACCEPTANCE.md) for deployment checks. Each distributed archive needs its own validation manifest.

After those gates close, make a reviewed release change: update version and lockfile together, write migration/changelog notes, record the supported SDK/server pair, resolve license/support metadata, and set registry/access deliberately. The current local release policy rejects stable versions and changed publication metadata until that policy itself is reviewed. There is no bypass flag.

## Future registry workflow

This is an operator design, not a configured publishing job. Use a protected release environment and trusted publishing once the package owner configures the exact repository/workflow identity in npm. Use a reviewed prerelease dist-tag such as `alpha` or `next`; reserve `latest` for the accepted stable version. Do not store long-lived registry tokens in this package or its examples. A local `npm pack` does not reserve a package name.

Before enabling publication, check the current [npm trusted publishing requirements](https://docs.npmjs.com/trusted-publishers/) for supported runners, Node/npm versions, repository/workflow identity and permissions. Publication credentials and OIDC permissions are not needed for the current artifact-only workflow.

Set access and the prerelease tag explicitly in the future publishing job; do not rely on registry defaults. Staged publishing is also a registry mutation and is not a substitute for this local preflight. After publishing, install the exact registry version in a clean consumer and run the supported smoke examples before promotion. [npm publish](https://docs.npmjs.com/cli/v11/commands/npm-publish/), [npm staged publishing](https://docs.npmjs.com/staged-publishing/).

If a version is defective, deprecate it with a clear replacement and publish a corrected version. Do not overwrite a published version or rely on unpublishing as the routine rollback. Preserve the tarball hash, CI identity, test evidence and SDK/server compatibility record for every released version. [npm deprecate](https://docs.npmjs.com/cli/v11/commands/npm-deprecate/).

## Version and migration policy

SDK SemVer, message-state versions and durable-event wire versions are separate. Alpha changes are recorded in the [changelog](../CHANGELOG.md) and [migration guide](MIGRATION.md). After 1.0, breaking public types, a higher minimum Node version, or weaker delivery guarantees require a major release; additive methods/events may be minor releases. Security fixes should document any compatibility impact.

Node 22/24 are the configured CI lanes. The declared engine is `>=22`, but that is not a claim that every later Node release has been tested. Explicitly opt into best-effort recovery on older supported servers; do not silently change recovery guarantees during an upgrade.
