// Package framework is the code the data services of the data-api stack
// share: each keeps a SQLite copy of DolphinDB tables, answers one query from
// it over HTTP, and has an MCP adapter that relays the query as one tool.
// The services require this module at a tagged version. Its tests are under
// test/, with harness, the helper the services' end-to-end tests run their
// binaries with.
//
//   - problem: the RFC 9457 error body and its codes
//   - endpoint: a query's parameters and fields, JSON, running HTTP
//   - timezone: Beijing time, the time zone of the source tables
//   - upstream: reading the source tables from DolphinDB
//   - replica: the SQLite copy of the source tables and its synchronization
//   - service: a data service binary around its query
//   - adapter: the MCP adapter binary that relays the query as one tool
//   - buildinfo: the version the binary reports, stamped at build time
package framework
