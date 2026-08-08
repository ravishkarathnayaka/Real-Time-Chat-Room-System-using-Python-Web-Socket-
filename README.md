# 💬 Real-Time Chat Room System

A real-time multi-room chat application built with **Python, AsyncIO, and WebSockets**, demonstrating practical concepts in network communication, asynchronous programming, client-server architecture, and real-time messaging.

> Developed as part of the CCS1305 – Communication Protocols & Models Mini Project.

---

## 📌 Project Overview

This project implements a lightweight real-time chat system using a WebSocket-based client-server architecture.

Multiple users can connect to a central server, “identify themselves using unique usernames”, join different chat rooms, exchange messages in real time, and retrieve recent message history.

The project was developed to explore how persistent bidirectional communication works between clients and servers using modern communication protocols.

---

## ✨ Features

- Real-time WebSocket communication
- Multi-user chat
- Multiple chat rooms
- Unique username handling
- Room subscription system
- Real-time message broadcasting
- Recent message history
- Persistent room-based message logs
- JSON-based communication protocol
- Graceful login and logout handling
- Configurable server environment
- Command-line chat client

---

## 🏗️ Architecture

The application follows a simple client-server architecture:

```text
┌──────────────┐
│   Client A   │
└──────┬───────┘
       │
       │ WebSocket
       │
┌──────▼───────────────┐
│                      │
│     Chat Server      │
│                      │
│  • Authentication    │
│  • Room Management   │
│  • Broadcasting      │
│  • Message History   │
│  • Logging           │
│                      │
└──────┬───────────────┘
       │
       │ WebSocket
       │
┌──────▼───────┐
│   Client B   │
└──────────────┘
```
The server maintains active WebSocket connections and manages users, room subscriptions, message history, and message broadcasting.

---

## 🛠️ Technology Stack

- **Python**
- **AsyncIO**
- **WebSockets**
- **JSON**
- **TCP/IP Networking Concepts**
- **Client-Server Architecture**

---

## 📂 Project Structure

```text
.
├── chat_server.py
├── chat_client.py
├── logs/                  # Created automatically at runtime
└── README.md
```

### `chat_server.py`

Handles:

- WebSocket connections
- User authentication
- Chat-room subscriptions
- Message broadcasting
- Message history
- Persistent logging
- Client cleanup

### `chat_client.py`

Provides a command-line client for:

- Connecting to the server
- Logging in
- Joining rooms
- Sending messages
- Receiving messages
- Viewing recent room history
- Logging out

---

## 🚀 Running the Project

### 1. Install Python

Python 3.10+ is recommended.

### 2. Install the WebSockets package

```bash
pip install websockets
```

### 3. Start the server

```bash
python chat_server.py
```

By default, the server runs on:

```text
0.0.0.0:2024
```

### 4. Start a client

Open another terminal:

```bash
python chat_client.py --username Alice
```

To join one or more rooms immediately:

```bash
python chat_client.py --username Alice --rooms general
```

Open additional terminals with different usernames to simulate multiple users.

---

## 💬 Client Commands

### Join a room

```text
/join general
```

### Send a message to a room

```text
general: Hello everyone!
```

After selecting a room, messages can also be sent directly:

```text
Hello everyone!
```

### Exit the application

```text
/quit
```

---

## 🔄 Communication Flow

```text
Client
  │
  ├── Login
  ▼
Server
  │
  ├── Validate Username
  ▼
Client
  │
  ├── Subscribe to Room
  ▼
Server
  │
  ├── Return Recent History
  ▼
Client
  │
  ├── Publish Message
  ▼
Server
  │
  ├── Store Message
  ├── Log Message
  └── Broadcast to Room Members
```

---

## ⚙️ Server Configuration

The server supports environment-based configuration.

| Variable | Purpose | Default |
|---|---|---|
| `CHAT_HOST` | Server host | `0.0.0.0` |
| `CHAT_PORT` | Server port | `2024` |
| `CHAT_LOG_DIR` | Message log directory | `logs` |
| `CHAT_HISTORY_SIZE` | Maximum cached message history | `100` |
| `CHAT_HISTORY_ON_SUBSCRIBE` | Recent messages returned when joining | `5` |

---

## 🎯 Learning Outcomes

This project provided practical experience with:

- WebSocket communication
- Client-server architecture
- Asynchronous programming with AsyncIO
- Real-time data transmission
- JSON-based communication
- Connection lifecycle management
- Multi-client communication
- Application-layer protocols
- Networking concepts
- Persistent message logging

---

## 🔮 Future Improvements

Potential improvements include:

- Graphical web interface
- Secure `wss://` connections
- User authentication
- Private messaging
- Database-backed message history
- Role-based chat rooms
- Docker deployment
- Cloud hosting
- Automated testing

---

## 👨‍💻 Author

**Ravishka Rathnayaka**

Cybersecurity Undergraduate  
SLTC

---

> This project was developed for educational purposes as part of the CCS1305 – Communication Protocols & Models Mini Project.
