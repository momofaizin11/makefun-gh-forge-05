# github-core-clone-01 — Build Notes

## Files Created
- LICENSE — MIT + SPDX-License-Identifier: MIT
- .gitignore — Python template
- main.py — Full stdlib implementation
- NOTES.md — This file

## Implementation Progress (Patches)

### Patch 1: Core Infrastructure ✓
- Imports, config constants, deterministic ID counter, ISO timestamp
- Verified: `python -m py_compile main.py` → OK

### Patch 2: TTLCache ✓
- Thread-safe OrderedDict with TTL expiry, injectable clock for testing
- Capacity bound (1000 entries), LRU eviction
- Verified: self-test cache TTL expiry works

### Patch 3: ConnectionPool ✓
- Keyed by host:port:ssl, max connections per host, timeout
- Reuses HTTP/HTTPS connections from http.client
- Note: pooling is connection reuse, not full connection pool with health checks

### Patch 4: Store (In-Memory Data) ✓
- User, Repository, Issue, PullRequest dataclasses
- Thread-safe with locks
- Seeded test user (octocat) and repo (Hello-World)
- CRUD for users, repos, issues, PRs
- Git object storage (sha -> bytes) and refs (ref -> sha)
- Verified: self-test CRUD operations pass

### Patch 5: Git Pack/Unpack ✓
- pkt-line parsing/writing (flush, data lines)
- build_packfile: simple pack v2 header + objects + SHA-1 trailer
- unpack_packfile: parse header, varint sizes, zlib decompress, reconstruct objects
- Ref advertisement parsing
- Verified: self-test packfile roundtrip passes

### Patch 6: HTTP Handler + Routing ✓
- BaseHTTPRequestHandler subclass with class-level route registry
- JSON responses, error handling, auth via Bearer token (dev format: user:login)
- Method dispatch: GET, POST, PATCH, DELETE, PUT

### Patch 7: REST v3 Routes ✓
- GET /user (authenticated)
- GET /users/:login (public)
- GET /repos/:owner/:name
- GET /users/:owner/repos
- POST /user/repos (auth)
- GET /repos/:owner/:name/issues (state filter)
- GET /repos/:owner/:name/issues/:number
- POST /repos/:owner/:name/issues (auth)
- PATCH /repos/:owner/:name/issues/:number (auth)
- GET /repos/:owner/:name/pulls (state filter)
- GET /repos/:owner/:name/pulls/:number
- POST /repos/:owner/:name/pulls (auth)
- PATCH /repos/:owner/:name/pulls/:number (auth)
- PUT /repos/:owner/:name/pulls/:number/merge (auth)

### Patch 8: GraphQL v4 ✓
- POST /graphql
- Handles viewer { login } query
- Handles repository(owner,name) { id name owner description } query
- Variables support
- Returns errors for unsupported queries

### Patch 9: MCP (Model Context Protocol) ✓
- GET /mcp/manifest — tool list
- POST /mcp/tool/create_issue
- POST /mcp/tool/create_pr
- POST /mcp/tool/list_repos
- All require auth

### Patch 10: Git Smart HTTP ✓
- GET /:owner/:name/info/refs?service=git-upload-pack|git-receive-pack
  - Ref advertisement with pkt-line format
- POST /:owner/:name/git-upload-pack
  - Parses want lines, builds packfile with requested objects
- POST /:owner/:name/git-receive-pack
  - Parses ref update commands, unpacks packfile, updates refs
  - Returns status report

### Patch 11: Self-Tests ✓
- run_tests() exercises all Store methods, git objects, refs, packfile, cache, IDs
- All tests pass

## Not Implemented / Known Gaps
- GraphQL: only 2 query patterns supported (viewer, repository)
- Git: no delta compression in packfiles, no shallow fetch, no negotiation
- Git: receive-pack lacks pre-receive/post-receive hooks
- Auth: dev-only token format, no OAuth, no real token validation
- Rate limiting: not implemented
- Pagination: not implemented (returns all)
- Webhooks: not implemented
- Gists, Actions, Projects, Packages: not implemented
- SSL/TLS: uses http.client HTTPSConnection but no cert verification config

## Verification
- `python -m py_compile main.py` — OK
- `python main.py --test` — All self-tests pass
- Server starts on `python main.py` (Ctrl-C to stop)

## Deterministic Design
- IDs: counter-based (id_000001, id_000002, ...)
- Cache clock: injectable for tests
- Output ordering: sorted() where sets used
- No random, no time.time() for IDs