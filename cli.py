from __future__ import annotations
import argparse
import json
import sys

import httpx

BASE_URL = "http://localhost:8000"


def cmd_chat(session_id: str, content: str) -> None:
    with httpx.Client(timeout=60.0) as client:
        resp = client.post(
            f"{BASE_URL}/chat",
            json={"session_id": session_id, "content": content},
        )
        resp.raise_for_status()
        request_id = resp.json()["request_id"]
        print(f"[request_id: {request_id}]")
        print("Assistant: ", end="", flush=True)

        with client.stream("GET", f"{BASE_URL}/chat/stream/{request_id}") as stream:
            for line in stream.iter_lines():
                if not line.startswith("data:"):
                    continue
                payload = line[len("data:"):].strip()
                if payload == "[DONE]":
                    print()
                    break
                data = json.loads(payload)
                token = data["choices"][0]["delta"]["content"]
                print(token, end="", flush=True)


def cmd_history(session_id: str) -> None:
    with httpx.Client() as client:
        resp = client.get(f"{BASE_URL}/history/{session_id}")
        resp.raise_for_status()
        messages = resp.json()
    if not messages:
        print(f"No history for session '{session_id}'")
        return
    for msg in messages:
        role = msg["role"].upper()
        content = msg["content"]
        print(f"[{role}] {content}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Event-driven chat CLI")
    subs = parser.add_subparsers(dest="command", required=True)

    chat_p = subs.add_parser("chat", help="Send a message and stream the response")
    chat_p.add_argument("--session", required=True, help="Session ID")
    chat_p.add_argument("content", help="Message to send")

    hist_p = subs.add_parser("history", help="Print conversation history")
    hist_p.add_argument("session_id", help="Session ID")

    args = parser.parse_args()

    if args.command == "chat":
        cmd_chat(session_id=args.session, content=args.content)
    elif args.command == "history":
        cmd_history(session_id=args.session_id)


if __name__ == "__main__":
    main()
