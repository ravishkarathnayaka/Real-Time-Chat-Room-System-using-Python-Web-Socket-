import asyncio
import json
import sys
from argparse import ArgumentParser
from typing import List

import websockets


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Simple WebSocket chat client")
    parser.add_argument("--host", default="localhost", help="Server host (default: localhost)")
    parser.add_argument("--port", type=int, default=2024, help="Server port (default: 2024)")
    parser.add_argument("--username", required=True, help="Unique username")
    parser.add_argument("--rooms", nargs="*", default=[], help="Rooms to subscribe to on start")
    return parser


async def receiver(ws: websockets.WebSocketClientProtocol) -> None:
    async for raw in ws:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            print(raw)
            continue

        msg_type = data.get("type")
        if msg_type == "error":
            print(f"[ERROR] {data.get('code')}: {data.get('message')}")
        elif msg_type == "ok":
            event = data.get("event")
            if event == "login_ok":
                print(f"Logged in as {data.get('username')}")
            elif event == "subscribed":
                print(f"Subscribed to room {data.get('room')}")
            elif event == "logout_ok":
                print("Logged out.")
        elif msg_type == "history":
            room = data.get("room")
            messages: List[dict] = data.get("messages", [])
            if messages:
                print(f"--- Last {len(messages)} messages in {room} ---")
                for m in messages:
                    print(f"[{m.get('ts')}] {m.get('room')} | {m.get('username')}: {m.get('message')}")
                print("-------------------------------------------")
        elif msg_type == "message":
            print(f"[{data.get('ts')}] {data.get('room')} | {data.get('username')}: {data.get('message')}")
        else:
            print(raw)


async def stdin_publisher(ws: websockets.WebSocketClientProtocol) -> None:
    print("Type '/join ROOM' to subscribe to a room. Type '/quit' to exit.")
    print("Publish with 'ROOM: your message' or just 'your message' to send to last used room.")
    last_room = None
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            await ws.close()
            return
        line = line.strip()
        if line == "":
            continue
        if line.startswith("/quit"):
            await ws.send(json.dumps({"action": "logout"}))
            await ws.close()
            return
        if line.startswith("/join "):
            _, room = line.split(" ", 1)
            await ws.send(json.dumps({"action": "subscribe", "room": room.strip()}))
            last_room = room.strip()
            continue

        # Publish: support 'room: message' or default last_room
        if ":" in line:
            room, message = line.split(":", 1)
            room = room.strip()
            message = message.strip()
            last_room = room
        else:
            if not last_room:
                print("No room selected. Use '/join ROOM' or 'ROOM: message'.")
                continue
            room = last_room
            message = line

        await ws.send(json.dumps({"action": "publish", "room": room, "message": message}))


async def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    uri = f"ws://{args.host}:{args.port}"
    async with websockets.connect(uri) as ws:
        # Login
        await ws.send(json.dumps({"action": "login", "username": args.username}))
        # Subscribe initial rooms
        for room in args.rooms:
            await ws.send(json.dumps({"action": "subscribe", "room": room}))

        # Run receiver and stdin publisher concurrently
        await asyncio.gather(receiver(ws), stdin_publisher(ws))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
