import asyncio
import os
import sys

from threadify import Threadify
from threadify.models import WaitOptions


async def main():
    api_key = os.getenv("THREADIFY_API_KEY", "your-api-key")

    if len(sys.argv) < 2:
        print("Usage: python async_notif.py <thread-id>")
        return

    thread_id = sys.argv[1]

    async with await Threadify.connect(api_key) as conn:
        # 1. Join an existing thread
        thread = await conn.join(thread_id=thread_id)
        print(f"Joined thread {thread_id}. Waiting for 'inventory_reserved'...")

        # 2. Wait for a specific step from another service
        try:
            # wait_for returns a Notification object
            notif = await thread.wait_for(
                "inventory_reserved", WaitOptions(timeout=60, statuses=["success"])
            )

            print(f"Got notification: {notif.message}")
            print(f"Data: {notif.details}")

            # 3. Acknowledge the notification (optional but recommended)
            notif.ack()
            print("Notification acknowledged.")

        except asyncio.TimeoutError:
            print("Timed out waiting for inventory reservation.")


if __name__ == "__main__":
    asyncio.run(main())
