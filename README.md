# Threadify Python SDK

Python SDK for connecting to the Threadify Engine over WebSocket and querying archived data over GraphQL.

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
            ws_url="wss://eng.threadify.dev/threads",
        )
    except Exception as e:
        logging.error(f"Failed to connect: {e}")
        return

    try:
        thread = await conn.start(contract_name="order_flow")
        
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

`ws_url` is required. The SDK no longer uses a hardcoded default URL.

## Configuration

### Connect options

Use keyword arguments with `Threadify.connect(...)`:

- `service_name`
- `ws_url` (required)
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
    ws_url="wss://eng.threadify.dev/threads",
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

## Testing

To run the SDK tests, execute:

```bash
make test
```

Alternatively, use `pytest`:

```bash
python3 -m pytest
```
