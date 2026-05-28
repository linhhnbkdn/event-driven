from __future__ import annotations
import uuid

from locust import HttpUser, between, task, constant


class ChatUser(HttpUser):
    """Simulates a user sending chat messages and streaming responses."""
    wait_time = constant(0)  # no wait between tasks — max load

    def on_start(self) -> None:
        self.session_id = str(uuid.uuid4())

    @task
    def chat_and_stream(self) -> None:
        with self.client.post(
            "/chat",
            json={"session_id": self.session_id, "content": "throughput là gì?"},
            catch_response=True,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"POST /chat {resp.status_code}")
                return
            request_id = resp.json().get("request_id")

        with self.client.get(
            f"/chat/stream/{request_id}",
            stream=True,
            catch_response=True,
            timeout=30,
        ) as stream:
            if stream.status_code != 200:
                stream.failure(f"stream {stream.status_code}")
                return
            for line in stream.iter_lines():
                if b"[DONE]" in line:
                    stream.success()
                    break
