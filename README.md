# Threadify Python SDK

Python SDK for [Threadify](https://threadify.dev) — Service-delivery intelligence that tracks every customer request from start to finish across every system, team, and partner.

- **Documentation:** [https://docs.threadify.dev](https://docs.threadify.dev)
- **Create an account:** [https://threadify.dev](https://threadify.dev)

## Installation

```bash
pip install threadify-sdk
```

For local development:

```bash
pip install -e ".[dev]"
```

## Quick Start

```python
import asyncio
import logging
from threadify import Threadify


async def main():
    try:
        conn = await Threadify.connect(
            "your-api-key",
            service_name="orders-service",
        )
    except Exception as e:
        logging.error(f"Failed to connect: {e}")
        return

    try:
        thread = await conn.start("Order-123")
        
        # Easy chaining!
        await (
            thread.step("order_received")
            .add_context({"orderId": "ORD-123"})
            .success("Order accepted")
        )
    except Exception as e:
        logging.error(f"Error in thread: {e}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
```

`ws_url` defaults to `wss://eng.threadify.dev/threads`. Override it if you are using a self-hosted or regional endpoint.

## Configuration

### Connect options

Use keyword arguments with `Threadify.connect(...)`:

- `service_name`
- `ws_url` (optional, defaults to production)
- `graphql_url`
- `debug`
- `max_in_flight`
- `connect_timeout`

Example:

```python
from threadify import Threadify

conn = await Threadify.connect(
    "your-api-key",
    service_name="inventory-service",
    debug=True,
)
```

### Join options

Use `Connection.join(...)` with keyword arguments:

- `token=...`
- `thread_id=...` and `role=...`

Example:

```python
thread = await conn.join(token="jwt-token")
```

Or:

```python
thread = await conn.join(
    thread_id="thread-123",
    role="supplier",
)
```

## Subscriptions

Preferred API:

- `subscribe(event, step_name, handler)`
- `unsubscribe(event, step_name)`

Supported event patterns:

- `step.success`
- `step.failed`
- `step.*`
- `rule.passed`
- `rule.violated`
- `rule.*`
- `*`

Example:

```python
def handle_notification(notification):
    if notification.is_violated:
        print(notification.message)



conn.subscribe("rule.violated", "payment_step", handle_notification)
```

## Versioning & Releases

This SDK follows [Semantic Versioning](https://semver.org/) and [Conventional Commits](https://www.conventionalcommits.org/). Releases are automated via GitHub Actions.

- `fix: ...` -> Patch bump
- `feat: ...` -> Minor bump
- `feat!: ...` or `BREAKING CHANGE: ...` -> Major bump

## OpenTelemetry Integration

The Python SDK includes the OpenTelemetry SpanExporter in the core package.

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from threadify import Threadify

conn = await Threadify.connect("api-key", service_name="checkout-service")

# Create exporter
exporter = conn.create_span_exporter(options={"refs": ["order.id", "customer.id"]})

# Filter spans by name — exact match or prefix wildcard with *
exporter = conn.create_span_exporter(options={
    "refs": ["order.id", "customer.id"],
    "filters": ["invoke_llm", "adk.before*", "llm.*"],
})

provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)
```

**Filter patterns:**

- `"invoke_llm"` — exact match
- `"adk.before*"` — prefix wildcard, drops any span starting with `adk.before`
- `"llm.*"` — prefix wildcard, drops any span starting with `llm.`

## Testing

To run the SDK tests, execute:

```bash
make test
```

Alternatively, use `pytest`:

```bash
python3 -m pytest
```
