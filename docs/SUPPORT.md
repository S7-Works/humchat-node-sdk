# Support and security during the alpha

The source repository is public at [S7-Works/humchat-node-sdk](https://github.com/S7-Works/humchat-node-sdk). Use its [issue tracker](https://github.com/S7-Works/humchat-node-sdk/issues) for nonsensitive bugs and questions. This is an alpha; no support SLA or response-time commitment is offered.

For a reproducible bug, include the package version, Node version, server version/capabilities, the failing method, the typed error code/status/request ID, and a minimal example with synthetic message content. For recovery problems, include the recovery mode, channel count, whether a persistent store is in use, and whether the worker restarted. Exclude tokens, client secrets, refresh tokens, message bodies, checkpoint/outbox files, and personally identifying data. See [troubleshooting](TROUBLESHOOTING.md).

For a suspected security defect, [submit a private vulnerability report](https://github.com/S7-Works/humchat-node-sdk/security/advisories/new) to the repository maintainers. Private vulnerability reporting is enabled. Include the affected version, impact and a minimal reproduction using synthetic data. Do not post exploit details, credentials or private deployment information in public issues.

A named security response owner and response-time commitment remain unassigned. The private reporting route does not establish a support SLA or production readiness.

The current [license](../LICENSE) is `UNLICENSED`; public source visibility does not grant an open-source license. Keeping `private: true` prevents npm publication. Neither a successful local test run nor possession of a tarball establishes npm scope ownership, production readiness or support commitments.
