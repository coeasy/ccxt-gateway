# Northflank service profile

This profile targets a Northflank **Combined Service** linked to the `main` branch of `coeasy/ccxt-gateway`.

## Build

- Build type: Dockerfile
- Build context: `/`
- Dockerfile: `/Dockerfile`
- Continuous integration: enabled
- Continuous delivery: enabled after the first successful build

## Runtime

- Container port: `8080` (the application also honors the assigned `PORT` environment variable)
- Public protocol: HTTP
- Instances: `1` while WebSocket fan-out is held in process memory
- Required runtime secret: `API_KEY`
- Optional runtime secret: `EXCHANGE_CREDENTIALS_JSON`
- Optional variables: `EXCHANGE_IDLE_TTL_MS`, `EXCHANGE_MAX_INSTANCES`, `EXCHANGE_TIMEOUT_MS`, `WS_IDLE_GRACE_MS`, `WS_MAX_CLIENTS`, `WS_MAX_STREAMS`

Do not add exchange keys as build arguments. Configure them as runtime secrets. The gateway API key should be generated in the deployment environment and must not be copied into this repository.

## Health checks

Configure HTTP probes on the application port:

| Probe | Path | Initial delay | Interval | Timeout | Failure threshold |
| --- | --- | ---: | ---: | ---: | ---: |
| Startup | `/health/ready` | 5 s | 5 s | 3 s | 12 |
| Liveness | `/health/live` | 10 s | 30 s | 3 s | 3 |
| Readiness | `/health/ready` | 5 s | 10 s | 3 s | 3 |

Northflank stores service, project, port, and secret settings in the account rather than reading a service manifest from this repository. Configure these values in the service UI or Northflank API after connecting the Git repository. The workflow above follows [Northflank's combined-service deployment flow](https://northflank.com/docs/v1/application/getting-started/build-and-deploy-your-code), [Dockerfile build settings](https://northflank.com/docs/v1/application/build/build-with-a-dockerfile), and [health-check configuration](https://northflank.com/docs/v1/application/observe/configure-health-checks).
