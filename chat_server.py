import asyncio
import json
import os
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Deque, Dict, Set

import websockets


HOST = os.environ.get("CHAT_HOST", "0.0.0.0")
PORT = int(os.environ.get("CHAT_PORT", "2024"))
LOG_DIR = os.path.abspath(os.environ.get("CHAT_LOG_DIR", os.path.join(os.getcwd(), "logs")))
HISTORY_SIZE = int(os.environ.get("CHAT_HISTORY_SIZE", "100"))
HISTORY_ON_SUBSCRIBE = int(os.environ.get("CHAT_HISTORY_ON_SUBSCRIBE", "5"))


class ChatServer:
    def __init__(self) -> None:
        os.makedirs(LOG_DIR, exist_ok=True)
        # Username to websocket mapping ensures uniqueness
        self.username_to_ws: Dict[str, websockets.WebSocketServerProtocol] = {}
        # Websocket to username reverse mapping
        self.ws_to_username: Dict[websockets.WebSocketServerProtocol, str] = {}
        # Room to set of websockets (subscribers)
        self.room_to_subscribers: Dict[str, Set[websockets.WebSocketServerProtocol]] = defaultdict(set)
        # Websocket to subscribed rooms
        self.ws_to_rooms: Dict[websockets.WebSocketServerProtocol, Set[str]] = defaultdict(set)
        # In-memory recent history per room (bounded)
        self.room_to_history: Dict[str, Deque[dict]] = defaultdict(lambda: deque(maxlen=HISTORY_SIZE))

    async def handler(self, websocket: websockets.WebSocketServerProtocol) -> None:
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    await self._send_error(websocket, "invalid_json", "Message must be valid JSON")
                    continue

                action = data.get("action")
                if action == "login":
                    await self._handle_login(websocket, data)
                elif action == "subscribe":
                    await self._handle_subscribe(websocket, data)
                elif action == "publish":
                    await self._handle_publish(websocket, data)
                elif action == "logout":
                    await self._handle_logout(websocket)
                else:
                    await self._send_error(websocket, "unknown_action", f"Unknown action: {action}")
        except websockets.ConnectionClosed:
            # Ensure clean-up on disconnect
            await self._cleanup(websocket)

    async def _handle_login(self, websocket: websockets.WebSocketServerProtocol, data: dict) -> None:
        username = data.get("username")
        if not username or not isinstance(username, str):
            await self._send_error(websocket, "invalid_username", "'username' must be a non-empty string")
            return

        if username in self.username_to_ws:
            await self._send_error(websocket, "username_taken", "Username already in use")
            # Close to force client to retry
            await websocket.close(code=4000, reason="username_taken")
            return

        # Link mappings
        self.username_to_ws[username] = websocket
        self.ws_to_username[websocket] = username
        await self._send_ok(websocket, "login_ok", {"username": username})

    async def _handle_subscribe(self, websocket: websockets.WebSocketServerProtocol, data: dict) -> None:
        room = data.get("room")
        if not room or not isinstance(room, str):
            await self._send_error(websocket, "invalid_room", "'room' must be a non-empty string")
            return

        # Register subscription
        self.room_to_subscribers[room].add(websocket)
        self.ws_to_rooms[websocket].add(room)
        await self._send_ok(websocket, "subscribed", {"room": room})

        # Send last N messages (history) to the subscriber
        await self._send_recent_history(websocket, room, HISTORY_ON_SUBSCRIBE)

    async def _handle_publish(self, websocket: websockets.WebSocketServerProtocol, data: dict) -> None:
        room = data.get("room")
        message = data.get("message")
        if not room or not isinstance(room, str):
            await self._send_error(websocket, "invalid_room", "'room' must be a non-empty string")
            return
        if not isinstance(message, str) or message == "":
            await self._send_error(websocket, "invalid_message", "'message' must be a non-empty string")
            return

        username = self.ws_to_username.get(websocket)
        if not username:
            await self._send_error(websocket, "not_logged_in", "Login required before publishing")
            return

        timestamp = datetime.now(timezone.utc).isoformat()
        record = {"type": "message", "room": room, "username": username, "message": message, "ts": timestamp}

        # Append to in-memory history and persistent log
        self.room_to_history[room].append(record)
        self._append_to_room_log(room, record)

        # Broadcast to all subscribers of the room
        await self._broadcast(room, record)

    async def _handle_logout(self, websocket: websockets.WebSocketServerProtocol) -> None:
        await self._send_ok(websocket, "logout_ok", {})
        await self._cleanup(websocket)
        await websocket.close(code=1000, reason="logout")

    async def _send_recent_history(self, websocket: websockets.WebSocketServerProtocol, room: str, count: int) -> None:
        # Ensure in-memory cache is seeded from disk if empty
        if len(self.room_to_history[room]) == 0:
            for rec in self._read_last_lines_from_log(room, HISTORY_SIZE):
                self.room_to_history[room].append(rec)

        # Prepare last N from in-memory
        recent = list(self.room_to_history[room])[-count:]
        if recent:
            await websocket.send(json.dumps({"type": "history", "room": room, "messages": recent}))

    async def _broadcast(self, room: str, record: dict) -> None:
        subs = list(self.room_to_subscribers.get(room, set()))
        if not subs:
            return
        payload = json.dumps(record)
        # Send concurrently
        await asyncio.gather(*[self._safe_send(ws, payload) for ws in subs])

    async def _safe_send(self, websocket: websockets.WebSocketServerProtocol, payload: str) -> None:
        try:
            await websocket.send(payload)
        except websockets.ConnectionClosed:
            await self._cleanup(websocket)

    async def _send_ok(self, websocket: websockets.WebSocketServerProtocol, event: str, data: dict) -> None:
        await websocket.send(json.dumps({"type": "ok", "event": event, **data}))

    async def _send_error(self, websocket: websockets.WebSocketServerProtocol, code: str, message: str) -> None:
        await websocket.send(json.dumps({"type": "error", "code": code, "message": message}))

    def _append_to_room_log(self, room: str, record: dict) -> None:
        path = self._room_log_path(room)
        line = f"{record['ts']}|{record['username']}|{record['message']}\n"
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)

    def _read_last_lines_from_log(self, room: str, count: int):
        path = self._room_log_path(room)
        if not os.path.exists(path):
            return []

        # Efficiently read last N lines
        lines: Deque[str] = deque(maxlen=count)
        with open(path, "rb") as f:
            # Read in chunks from end
            f.seek(0, os.SEEK_END)
            buffer = bytearray()
            pointer = f.tell()
            while pointer > 0 and len(lines) < count:
                read_size = min(4096, pointer)
                pointer -= read_size
                f.seek(pointer)
                chunk = f.read(read_size)
                buffer[:0] = chunk
                while b"\n" in buffer:
                    idx = buffer.rfind(b"\n")
                    line_bytes = buffer[idx + 1 :]
                    buffer = buffer[:idx]
                    if line_bytes:
                        lines.appendleft(line_bytes.decode("utf-8", errors="ignore"))
                if pointer == 0 and buffer:
                    lines.appendleft(buffer.decode("utf-8", errors="ignore"))
                    buffer.clear()

        # Parse lines into records
        records = []
        for line in lines:
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                ts, username, msg = line.split("|", 2)
                records.append({"type": "message", "room": room, "username": username, "message": msg, "ts": ts})
            except ValueError:
                continue
        return records

    def _room_log_path(self, room: str) -> str:
        safe_room = "".join(c for c in room if c.isalnum() or c in ("-", "_")) or "room"
        return os.path.join(LOG_DIR, f"{safe_room}.txt")

    async def _cleanup(self, websocket: websockets.WebSocketServerProtocol) -> None:
        # Remove from username maps
        username = self.ws_to_username.pop(websocket, None)
        if username and self.username_to_ws.get(username) is websocket:
            self.username_to_ws.pop(username, None)

        # Remove from any room subscriptions
        rooms = self.ws_to_rooms.pop(websocket, set())
        for room in rooms:
            subs = self.room_to_subscribers.get(room)
            if subs and websocket in subs:
                subs.remove(websocket)
                if len(subs) == 0:
                    # Keep history map; subscribers set can remain empty
                    pass


async def main() -> None:
    server = ChatServer()
    async with websockets.serve(server.handler, HOST, PORT):
        print(f"Chat server running on {HOST}:{PORT}. Logs in {LOG_DIR}")
        await asyncio.Future()  # Run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
