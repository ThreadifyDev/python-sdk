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
        thread = await conn.start(
            contract_name="order_processing",
            role="customer",
            refs={"order_id": "ORD-123"},
            tags=["priority"],
        )

        await thread.add_refs({"crm_id": "CRM-456"})
        
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

For self-hosting, pass `engine_url="https://threadify.example.com"`. WebSocket and GraphQL paths are derived from that base, including reverse-proxy prefixes. Explicit `ws_url` and `graphql_url` remain available for split deployments.

## Entity profile config as code

The management client sends a complete desired declaration to Threadify. The
server performs metric reconciliation by name; SDK callers do not send database
metric IDs or calculate deletions.

```python
profiles = Threadify.entity_profiles(
    "your-service-api-key",
    web_api_url="https://web.threadify.dev/api",
)

declaration = {
    "name": "Customer",
    "description": "Customer delivery intelligence",
    "type": ["customer_id", "customer_email"],
    "metrics": [
        {
            "name": "Delivery success rate",
            "template_id": "delivery_success_rate",
            "parameters": {"window": "30d"},
        }
    ],
}

plan = await profiles.apply(declaration, dry_run=True)
result = await profiles.apply(declaration)
await profiles.rename("Customer", "Account")  # explicit identity change
await profiles.close()
```

The SDK can also load the declaration directly from YAML:

```yaml
# threadify-profile.yaml
name: Customer
description: Customer delivery intelligence
type:
  - customer_id
  - customer_email
metrics: []
```

```python
plan = await profiles.apply_file("threadify-profile.yaml", dry_run=True)
result = await profiles.apply_file("threadify-profile.yaml")
```

## Configuration

### Connect options

Use keyword arguments with `Threadify.connect(...)`:

- `service_name`
- `engine_url` (one HTTP(S) deployment base, including any proxy prefix)
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

### Start options

`Connection.start(...)` supports named options for contract, role, refs, and tags, while preserving the older positional forms:

```python
thread = await conn.start(
    contract_name="order_processing",
    role="customer",
    refs={"customer_id": "123"},
    tags=["priority"],
)
thread = await conn.start("Order-123", "customer")
thread = await conn.start({"customer_id": "123"}, "customer")
thread = await conn.start("Order-123")  # contract is optional
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


## Contract coordination (0.3)

```python
from threadify import Threadify, WaitOptions, ThreadifyError

connection = await Threadify.connect(api_key, engine_url="https://threadify.example.com", service_name="payments")
threads = await connection.get_threads_by_ref({"order_id": "ORD-1001"}, status="active", limit=25)
thread = await connection.join(thread_id, "processor")
grant = await thread.wait_for("charge", WaitOptions(timeout=15))
# Execute the permitted business operation here.
result = await thread.step("charge").add_context({"amount": 42}).success("charged", wait_for=True, timeout=15)
assert result.validation.decision == "passed"
# Resume validation of exactly this event if a previous caller stopped waiting.
await thread.wait_for_validation("charge", result.step_id)
```

`wait_for()` now asks the Engine for a permission grant, carrying an invocation
ID into the subsequent step report and its default idempotency key. It sends one
request and waits for a final correlated response. The old notification-only
helper is named `wait_for_notification()`; it does not authorize execution.

Timeouts are in **seconds**, at most 300. Cancelling the Python task or reaching
its timeout cancels the remote wait. Before reporting a granted operation, use
`await grant.cancel()` if you decide not to execute it. Cancellation does not
restore consumed fresh prerequisites; another invocation may need a new
successful predecessor. A disconnect rejects
pending requests immediately. No write is automatically retried; timeout errors
retain `invocation_id`, `idempotency_key`, or `step_id` where available for recovery.
A submitted event can still be persisted after its caller times out.

With `wait_for=True`, duplicate, violated, unavailable, and mismatched validation
responses raise `ThreadifyError` with a stable `code`; an ordinary acknowledgement
is not a validation result. Normal non-waiting duplicate reports keep the existing
`StepResult.duplicate` behavior. Contract definitions and Gherkin are enforced by
the Engine; SDKs select the contract and role rather than parsing contracts.

Install `threadify-sdk[otel]` to use the OTEL exporter. It preserves the recorded
span start/end and span-event timestamps, including spans exported later.

## Package CI and publishing

CI tests Python 3.10, 3.12, and 3.13, including OTEL and wait-protocol regressions.
The `pypi-publish.yml` workflow validates a matching `vX.Y.Z` tag, builds a wheel
and source archive, checks metadata and wheel imports, then publishes using
[PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/).
Configure a trusted publisher for owner `ThreadifyDev`, repository `python-sdk`,
workflow `pypi-publish.yml`, environment `pypi` in the `threadify-sdk` PyPI project.
A manual workflow run builds artifacts without publishing. No release is created
by running local tests or pushing an ordinary branch.
