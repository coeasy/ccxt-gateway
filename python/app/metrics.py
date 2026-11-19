from __future__ import annotations

BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)


def _label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class RequestMetrics:
    def __init__(self) -> None:
        self.requests: dict[tuple[str, str, int], dict[str, object]] = {}
        self.exchange_errors = 0
        self.storage_errors = 0

    def observe(self, method: str, route: str, status: int, duration: float) -> None:
        key = (method, route, status)
        metric = self.requests.setdefault(
            key, {"count": 0, "sum": 0.0, "buckets": [0] * len(BUCKETS)}
        )
        metric["count"] = int(metric["count"]) + 1
        metric["sum"] = float(metric["sum"]) + max(0.0, duration)
        counts = metric["buckets"]
        assert isinstance(counts, list)
        for index, bucket in enumerate(BUCKETS):
            if duration <= bucket:
                counts[index] += 1

    def render(self, cache_entries: int, exchange_instances: int, websocket_streams: int) -> str:
        lines = [
            "# HELP ccxt_gateway_http_requests_total Completed HTTP requests.",
            "# TYPE ccxt_gateway_http_requests_total counter",
            "# HELP ccxt_gateway_http_request_duration_seconds HTTP request duration in seconds.",
            "# TYPE ccxt_gateway_http_request_duration_seconds histogram",
            "# HELP ccxt_gateway_exchange_errors_total Failed upstream exchange requests.",
            "# TYPE ccxt_gateway_exchange_errors_total counter",
            "# HELP ccxt_gateway_storage_errors_total Failed market history or audit writes.",
            "# TYPE ccxt_gateway_storage_errors_total counter",
            "# HELP ccxt_gateway_cache_entries Current local cache entries.",
            "# TYPE ccxt_gateway_cache_entries gauge",
            "# HELP ccxt_gateway_exchange_instances Current open exchange instances.",
            "# TYPE ccxt_gateway_exchange_instances gauge",
            "# HELP ccxt_gateway_websocket_streams Current shared WebSocket streams.",
            "# TYPE ccxt_gateway_websocket_streams gauge",
        ]
        for (method, route, status), metric in self.requests.items():
            labels = f'method="{_label(method)}",route="{_label(route)}",status="{status}"'
            count = int(metric["count"])
            lines.append(f"ccxt_gateway_http_requests_total{{{labels}}} {count}")
            bucket_counts = metric["buckets"]
            assert isinstance(bucket_counts, list)
            for index, bucket in enumerate(BUCKETS):
                lines.append(
                    f"ccxt_gateway_http_request_duration_seconds_bucket{{{labels},"
                    f'le="{bucket}"}} {bucket_counts[index]}'
                )
            lines.append(
                f'ccxt_gateway_http_request_duration_seconds_bucket{{{labels},le="+Inf"}} {count}'
            )
            lines.append(
                f"ccxt_gateway_http_request_duration_seconds_sum{{{labels}}} {float(metric['sum'])}"
            )
            lines.append(f"ccxt_gateway_http_request_duration_seconds_count{{{labels}}} {count}")
        lines.extend(
            [
                f"ccxt_gateway_exchange_errors_total {self.exchange_errors}",
                f"ccxt_gateway_storage_errors_total {self.storage_errors}",
                f"ccxt_gateway_cache_entries {cache_entries}",
                f"ccxt_gateway_exchange_instances {exchange_instances}",
                f"ccxt_gateway_websocket_streams {websocket_streams}",
            ]
        )
        return "\n".join(lines) + "\n"
