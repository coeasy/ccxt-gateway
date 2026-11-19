# Review record

## Round 1 — authentication and secret handling

- Required a gateway key on every REST API route and on the first WebSocket frame; health probes remain public.
- Added a 32-character minimum and removed the example key value so copying `.env.example` cannot silently enable a known credential.
- Moved REST authentication to `onRequest` so unauthenticated calls are rejected before query/body validation.
- Removed raw upstream exception details from HTTP/WebSocket responses and logs. Exchange credentials are read only from runtime configuration; order and balance endpoints fail closed if credentials are absent.
- Verified invalid HTTP and WebSocket credentials, private-route gating, and limit-order input checks in the automated suite.
- `npm audit --omit=dev --registry=https://registry.npmjs.org`: 0 vulnerabilities.

## Round 2 — reuse, concurrency, and shutdown

- Added a shared in-flight constructor and reference-counted leases per exchange, idle TTL eviction, an instance cap, and deterministic shutdown.
- Added a pending-stream map so simultaneous subscribers for the same upstream channel share one watcher. Added per-socket subscription IDs, serialized frame processing, queue/subscription limits, slow-client handling, bounded reconnect backoff, and cleanup after the last subscriber leaves.
- Corrected the CCXT ESM import after a package compatibility check exposed that `ccxt.default` is not the CCXT module object.
- Verified concurrent REST leases, idle close, capacity handling, real CCXT Pro constructor availability, duplicate frame handling, shared WebSocket fan-out, bad authentication, and stream caps.

## Round 3 — container and deployment readiness

- Docker uses a production-only dependency install, a non-root runtime user, an explicit health check, and the platform-provided `PORT` when present.
- CI runs the check suite on Node.js 22 and 24 and builds the Docker image on pushes and pull requests.
- Added a Northflank service profile with Dockerfile build settings, runtime secret guidance, one-instance WebSocket settings, and HTTP probes.
- The Docker image could not be built locally because Docker CLI is unavailable in this environment. The GitHub Actions Docker job is the remote build verification. Northflank's account-bound service has not been created; its project, port, and secrets are configured in the Northflank account after linking the repository.
