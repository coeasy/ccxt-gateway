const BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

function escapeLabel(value) {
  return String(value).replaceAll('\\', '\\\\').replaceAll('"', '\\"').replaceAll('\n', '\\n');
}

export class RequestMetrics {
  constructor() {
    this.requests = new Map();
    this.exchangeErrors = 0;
    this.storageErrors = 0;
  }

  observe(method, route, status, durationSeconds) {
    const key = JSON.stringify([method, route, status]);
    const metric = this.requests.get(key) ?? { method, route, status, count: 0, sum: 0, buckets: new Array(BUCKETS.length).fill(0) };
    metric.count += 1;
    metric.sum += durationSeconds;
    BUCKETS.forEach((bucket, index) => { if (durationSeconds <= bucket) metric.buckets[index] += 1; });
    this.requests.set(key, metric);
  }

  render({ cacheEntries = 0, exchangeInstances = 0, websocketStreams = 0 } = {}) {
    const lines = [
      '# HELP ccxt_gateway_http_requests_total Completed HTTP requests.',
      '# TYPE ccxt_gateway_http_requests_total counter',
      '# HELP ccxt_gateway_http_request_duration_seconds HTTP request duration in seconds.',
      '# TYPE ccxt_gateway_http_request_duration_seconds histogram',
      '# HELP ccxt_gateway_exchange_errors_total Failed upstream exchange requests.',
      '# TYPE ccxt_gateway_exchange_errors_total counter',
      '# HELP ccxt_gateway_storage_errors_total Failed market history or audit writes.',
      '# TYPE ccxt_gateway_storage_errors_total counter',
      '# HELP ccxt_gateway_cache_entries Current local cache entries.',
      '# TYPE ccxt_gateway_cache_entries gauge',
      '# HELP ccxt_gateway_exchange_instances Current open exchange instances.',
      '# TYPE ccxt_gateway_exchange_instances gauge',
      '# HELP ccxt_gateway_websocket_streams Current shared WebSocket streams.',
      '# TYPE ccxt_gateway_websocket_streams gauge',
    ];
    for (const metric of this.requests.values()) {
      const labels = `method="${escapeLabel(metric.method)}",route="${escapeLabel(metric.route)}",status="${metric.status}"`;
      lines.push(`ccxt_gateway_http_requests_total{${labels}} ${metric.count}`);
      BUCKETS.forEach((bucket, index) => lines.push(`ccxt_gateway_http_request_duration_seconds_bucket{${labels},le="${bucket}"} ${metric.buckets[index]}`));
      lines.push(`ccxt_gateway_http_request_duration_seconds_bucket{${labels},le="+Inf"} ${metric.count}`);
      lines.push(`ccxt_gateway_http_request_duration_seconds_sum{${labels}} ${metric.sum}`);
      lines.push(`ccxt_gateway_http_request_duration_seconds_count{${labels}} ${metric.count}`);
    }
    lines.push(
      `ccxt_gateway_exchange_errors_total ${this.exchangeErrors}`,
      `ccxt_gateway_storage_errors_total ${this.storageErrors}`,
      `ccxt_gateway_cache_entries ${cacheEntries}`,
      `ccxt_gateway_exchange_instances ${exchangeInstances}`,
      `ccxt_gateway_websocket_streams ${websocketStreams}`,
    );
    return `${lines.join('\n')}\n`;
  }
}
