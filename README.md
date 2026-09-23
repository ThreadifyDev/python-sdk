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
        thread = await conn.thread("order:ORD-123", {
            "label": "Order ORD-123",
            "contract": "order_processing",
            "role": "customer",
            "refs": {"order_id": "ORD-123"},
            "tags": ["priority"],
        })

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

## Entity profiles

Track a customer's activity across workflows by setting up a profile type with
the [Threadify CLI](https://docs.threadify.dev/cli):

```yaml
# customers.yaml
name: Customers
type: [customer_id]
description: Customer workflows
```

```sh
threadify-cli config set api-url https://threadify.example.com
threadify-cli login
threadify-cli profile-types create --file customers.yaml
threadify-cli profiles create --type-id TYPE_ID --ref-value CUST-001 --name 'Jane Doe'
```

Use the returned profile type ID for `TYPE_ID`. Include `customer_id` in your
thread references to connect each workflow to its customer. Threadify can also
create profiles as matching activity arrives. Open **Entity Profiles** in the
dashboard to explore that history and configure metrics.

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

### Create or resume a thread

`await connection.thread(thread_key, options=None)` atomically creates or resumes
an application-owned key within the tenant. Use a session, order, or process ID
that your application already knows; you do not need to persist Threadify's
internal `thread_id` between requests.

```python
thread = await conn.thread("agent-session:123", {
    "label": "Agent session",
    "contract": "agent_contract",
    "refs": {"customer_id": "123"},
    "tags": ["priority"],
})

# A later request or another worker uses the same key without repeating options.
thread = await conn.thread("agent-session:123")
await thread.step("tool_call").add_context({"tool": "search"}).success()
```

The optional dictionary accepts `label`, `contract`, `refs`, `tags`,
`service_name`, and `role`. `ThreadOptions` is exported for type annotations.
Labels, contracts, service names, and roles must be non-empty strings when
supplied; refs map non-empty string keys to string values, and tags are a list
of non-empty strings.

Options are creation defaults. Resuming loads the stored label, refs, tags,
contract name, and pinned contract version into the returned handle. Repeating
an existing contract is allowed; supplying a conflicting contract fails without
changing the thread. Use `await thread.add_refs({...})` to change references
explicitly. Metadata is available as `thread.thread_key`, `thread.label`,
`thread.contract_name`, `thread.contract_version`, `thread.refs`, and `thread.tags`.

Keys are trimmed, case-sensitive strings of at most 1024 UTF-8 bytes. Concurrent
calls with the same key resolve to one thread. A key-only call on an unknown key
creates a free-form thread, so initialize contracted sessions before collectors
or other workers report activity. Closed, completed, and cancelled threads reject
acquisition and further writes; use a new key for a new process. `join()` remains
available for internal IDs and invitation tokens.

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

The Python SDK includes the OpenTelemetry SpanExporter in the core package. Use
`BatchSpanProcessor`: synchronous `export()` waits for Engine acknowledgements
on its worker thread and returns failure if any span fails or the deadline expires.
It continues attempting unrelated spans in a failed batch and does not complete
sessions from that batch. Calling synchronous export on the connection event
loop is rejected to avoid a deadlock. Before closing that loop or the connection,
run `await asyncio.to_thread(provider.shutdown)` to drain queued spans. Use
`await asyncio.to_thread(provider.force_flush)` for an explicit flush.


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

# Initialize before exporting spans when this session has a contract.
await conn.thread("agent-session:123", {"contract": "agent_contract"})
tracer = trace.get_tracer("agent")
with tracer.start_as_current_span("tool_call", attributes={
    "threadify.thread_key": "agent-session:123",
}):
    pass  # Perform the instrumented operation.
```

The exporter resolves `threadify.thread_key` using the same create-or-resume
operation as `connection.thread()`. Omit the contract on later spans; the stored
contract version is loaded automatically. Shared root spans keep the session open.
See [OTel correlation](OTEL_CORRELATION.md) for identity precedence and completion.

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
thread = await connection.thread("order:ORD-1001")
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
