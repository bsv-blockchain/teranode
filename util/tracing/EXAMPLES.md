# OpenTelemetry Exporter Configuration Examples

The `tracing_collector_url` setting controls where OpenTelemetry traces are exported. Here are examples for common observability backends:

## Jaeger (Local Development)

**OTLP HTTP endpoint:**
```conf
tracing_collector_url = http://localhost:4318/v1/traces
```

**Alternative Jaeger native endpoint:**
```conf
tracing_collector_url = http://localhost:14268/api/traces
```

## Datadog

**Datadog Agent (recommended):**
```conf
tracing_collector_url = http://localhost:8126/v0.4/traces
```

**Direct to Datadog (requires API key in headers - not implemented yet):**
```conf
tracing_collector_url = https://trace.agent.datadoghq.com/v0.4/traces
```

## Other OTLP-Compatible Backends

**Grafana Tempo:**
```conf
tracing_collector_url = http://localhost:3200/otlp/v1/traces
```

**New Relic:**
```conf
tracing_collector_url = https://otlp.nr-data.net:4318/v1/traces
```

**Honeycomb:**
```conf
tracing_collector_url = https://api.honeycomb.io/v1/traces/otlp
```

## Settings File Configuration

Add to your `settings.conf` or `settings_local.conf`:

```conf
# Enable tracing
tracing_enabled = true

# Set sample rate (0.01 = 1%)
tracing_SampleRate = 0.01

# Configure exporter endpoint
tracing_collector_url = http://localhost:4318/v1/traces
```

## Environment-Specific Examples

**Development (Jaeger):**
```conf
tracing_collector_url.dev = http://localhost:4318/v1/traces
```

**Production (Datadog):**
```conf
tracing_collector_url.prod = http://datadog-agent:8126/v0.4/traces
```

**Docker Compose:**
```conf
tracing_collector_url.docker = http://jaeger:4318/v1/traces
```