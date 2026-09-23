"""Python participant in the Engine's Python → Go → Python E2E test."""

import asyncio
import json
import os
import sys

from threadify import Threadify


async def main():
    conn = await Threadify.connect(
        os.environ["THREADIFY_API_KEY"],
        engine_url=os.environ["THREADIFY_ENGINE_URL"],
        service_name="python-checkout",
    )
    try:
        if sys.argv[1] == "create":
            thread = await conn.thread(
                f"parity:{os.environ['THREADIFY_PARITY_ID']}",
                {
                    "label": "SDK parity",
                    "contract": os.environ["THREADIFY_CONTRACT"],
                    "role": "python",
                    "refs": {"parity_id": os.environ["THREADIFY_PARITY_ID"]},
                },
            )
            result = await thread.step("approval").success("approved", wait_for=True)
            assert result.validation.decision == "passed"
            print(json.dumps({"thread_id": thread.thread_id, "step_id": result.step_id}))
        else:
            thread = await conn.thread(f"parity:{os.environ['THREADIFY_PARITY_ID']}")
            assert thread.thread_id == os.environ["THREADIFY_THREAD_ID"]
            validation = await thread.wait_for_validation("charge", os.environ["THREADIFY_STEP_ID"])
            assert validation.decision == "passed"
            await thread.wait_for("finish")
            result = await thread.step("finish").success("delivered", wait_for=True)
            assert result.validation.decision == "passed"
            for _ in range(100):
                found = await conn.get_threads_by_ref(
                    {"parity_id": os.environ["THREADIFY_PARITY_ID"]}
                )
                if found and found[0].status == "completed":
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("shared thread did not complete")
            print(json.dumps({"thread_id": thread.thread_id, "status": "completed"}))
    finally:
        await conn.close()


asyncio.run(main())
