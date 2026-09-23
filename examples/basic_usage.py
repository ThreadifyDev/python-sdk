import asyncio
import os

from threadify import Threadify


async def main():
    api_key = os.getenv("THREADIFY_API_KEY", "your-api-key")

    # 1. Connect to Threadify
    # The production WebSocket URL is used by default when ws_url is omitted.
    async with await Threadify.connect(api_key, service_name="orders-service") as conn:
        print("Connected to Threadify!")

        # 2. Create or resume the order using its durable application identifier.
        thread = await conn.thread(
            "order:ORD-PY-123",
            {
                "label": "Order ORD-PY-123",
                "contract": "order_processing",
                "role": "customer",
                "refs": {"order_id": "ORD-PY-123"},
                "tags": ["priority"],
            },
        )
        print(f"Thread ready: {thread.thread_id}")
        # Later requests can use conn.thread("order:ORD-PY-123") without the contract.
        # Once completed, this key rejects writes; use a new order ID for a new order.

        # 3. Record steps using the fluent API
        await (
            thread.step("order_received")
            .add_context(
                {"orderId": "ORD-PY-123", "customer": "Alice", "items": ["laptop", "mouse"]}
            )
            .success("Order received and validated")
        )
        print("Step 'order_received' recorded.")

        # 4. Add some references
        await thread.add_refs({"crm_id": "CRM-456"})

        # 5. Complete the thread
        resp = await thread.complete("All steps finished successfully")
        print(f"Thread completed at: {resp.ended_at}")


if __name__ == "__main__":
    asyncio.run(main())
