#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
server.py - Máy chủ game + WebSocket multiplayer (aiohttp)

Cài đặt & chạy:
    pip install aiohttp
    python server.py                 # http://localhost:8080   |   WebSocket: ws://localhost:8080/ws

Biến môi trường (tuỳ chọn):
    HOST=0.0.0.0  PORT=8080  GAME_HTML=aa.html  MAX_ROOM=16
    ALLOWED_ORIGINS=https://tenmien.com,https://www.tenmien.com   (chặn website lạ kết nối WS)

Giao thức (JSON, mỗi gói là 1 object có khoá "t"):
  Client -> Server
    {"t":"ping","ts":<số>}                                   đo độ trễ
    {"t":"join","room":"dm","name":"..","cls":"sword","race":"nhan"}   vào phòng (room="lobby" = sảnh)
    {"t":"s","x":..,"y":..,"f":..,"hp":..,"mhp":..,"al":1}   trạng thái, ~20 lần/giây
    {"t":"act","k":"a|s1|s2|ult|dash","a":<góc>}             hành động (đánh thường / kỹ năng)
  Server -> Client
    {"t":"welcome","id":"..","online_count":N}
    {"t":"joined","room":"dm","players":[{...}],"online_count":N}
    {"t":"join","p":{id,name,cls,race,x,y,f,hp,mhp,al}}      có người xuất hiện trong phòng
    {"t":"leave","id":".."}
    {"t":"s","id":"..","x":..,"y":..,"f":..,"hp":..,"mhp":..,"al":..}   phát tán trạng thái người khác
    {"t":"act","id":"..","k":"s1","a":1.2}
    {"t":"pong","ts":<echo>,"online_count":N}
    {"t":"online","online_count":N}                           số người online thay đổi
"""

import asyncio
import json
import math
import os
import re
import time
import uuid
from pathlib import Path

from aiohttp import web, WSMsgType

# ----------------------------------------------------------------------------
# Cấu hình
# ----------------------------------------------------------------------------
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))
HTML_PATH = Path(os.getenv("GAME_HTML", str(Path(__file__).with_name("aa.html"))))
MAX_ROOM = int(os.getenv("MAX_ROOM", "16"))           # số người tối đa / phòng (đầy -> tự tách phòng "dm#2")
ALLOWED_ORIGINS = {o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()}

LOBBY = "lobby"
ACTIONS = {"a", "s1", "s2", "ult", "dash"}
WORLD_MAX = 20000.0                                    # giới hạn toạ độ chấp nhận được
RATE_PER_SEC = 60.0                                    # giới hạn gói tin / giây / client
RATE_BURST = 120.0
OUTBOX_MAX = 256                                       # hàng đợi gửi / client (đầy -> bỏ gói cũ nhất)

ROOM_RE = re.compile(r"^[a-z0-9_]{1,16}$")
KEY_RE = re.compile(r"^[a-z0-9_]{1,16}$")
CTRL_RE = re.compile(r"[\x00-\x1f\x7f<>]")


def dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def num(v, lo, hi, default=0.0) -> float:
    """Ép về số hữu hạn trong [lo, hi]; sai kiểu / NaN / inf -> default."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return default
    v = float(v)
    if not math.isfinite(v):
        return default
    return max(lo, min(hi, v))


def clean_text(v, n=20) -> str:
    return CTRL_RE.sub("", str(v or ""))[:n].strip()


def clean_key(v, default="") -> str:
    return v if isinstance(v, str) and KEY_RE.match(v) else default


# ----------------------------------------------------------------------------
# Người chơi (1 kết nối WebSocket)
# ----------------------------------------------------------------------------
class Player:
    __slots__ = ("id", "ws", "outbox", "name", "cls", "race", "room", "has_state",
                 "x", "y", "f", "hp", "mhp", "al", "tokens", "t_last")

    def __init__(self, ws=None):
        self.id = uuid.uuid4().hex[:8]
        self.ws = ws
        self.outbox = asyncio.Queue(maxsize=OUTBOX_MAX)
        self.name, self.cls, self.race = "Người chơi", "sword", ""
        self.room = LOBBY
        self.has_state = False                         # đã gửi trạng thái đầu tiên trong phòng hiện tại chưa
        self.x = self.y = self.f = 0.0
        self.hp = self.mhp = 1
        self.al = 1
        self.tokens, self.t_last = RATE_BURST, time.monotonic()

    def send(self, msg: str):
        """Đẩy gói tin vào hàng đợi (không chặn). Hàng đợi đầy -> bỏ gói cũ nhất."""
        try:
            self.outbox.put_nowait(msg)
        except asyncio.QueueFull:
            try:
                self.outbox.get_nowait()
                self.outbox.put_nowait(msg)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    def allow(self) -> bool:
        now = time.monotonic()
        self.tokens = min(RATE_BURST, self.tokens + (now - self.t_last) * RATE_PER_SEC)
        self.t_last = now
        if self.tokens < 1:
            return False
        self.tokens -= 1
        return True

    def public(self) -> dict:
        return {"id": self.id, "name": self.name, "cls": self.cls, "race": self.race,
                "x": self.x, "y": self.y, "f": self.f, "hp": self.hp, "mhp": self.mhp, "al": self.al}


# ----------------------------------------------------------------------------
# Hub: quản lý phòng + xử lý gói tin (không phụ thuộc aiohttp nên dễ test)
# ----------------------------------------------------------------------------
class Hub:
    def __init__(self, max_room: int = MAX_ROOM):
        self.players = {}          # id -> Player
        self.rooms = {}            # tên phòng -> set(Player)   (không chứa sảnh)
        self.max_room = max_room
        self._online_dirty = False

    @property
    def online_count(self) -> int:
        return len(self.players)

    # --- vòng đời kết nối ---
    def add(self, p: Player):
        self.players[p.id] = p
        p.send(dumps({"t": "welcome", "id": p.id, "online_count": self.online_count}))
        self._online_dirty = True

    def remove(self, p: Player):
        self._leave(p)
        if self.players.pop(p.id, None) is not None:
            self._online_dirty = True

    def flush_online(self):
        """Phát số người online cho mọi client (gộp lại, gọi mỗi ~1 giây)."""
        if not self._online_dirty:
            return
        self._online_dirty = False
        msg = dumps({"t": "online", "online_count": self.online_count})
        for q in self.players.values():
            q.send(msg)

    # --- phòng ---
    def _assign(self, base) -> str:
        if not isinstance(base, str) or base == LOBBY or not ROOM_RE.match(base):
            return LOBBY
        n = 1
        while True:
            name = base if n == 1 else f"{base}#{n}"
            if len(self.rooms.get(name, ())) < self.max_room:
                return name
            n += 1

    def _broadcast(self, room: str, msg: str, exclude: Player = None):
        for q in self.rooms.get(room, ()):
            if q is not exclude:
                q.send(msg)

    def _leave(self, p: Player):
        if p.room == LOBBY:
            return
        members = self.rooms.get(p.room)
        if members is not None:
            members.discard(p)
            if p.has_state:
                self._broadcast(p.room, dumps({"t": "leave", "id": p.id}))
            if not members:
                del self.rooms[p.room]
        p.room, p.has_state = LOBBY, False

    # --- xử lý gói tin ---
    def on_message(self, p: Player, raw: str):
        if not p.allow():
            return
        try:
            d = json.loads(raw)
        except (ValueError, TypeError):
            return
        if not isinstance(d, dict):
            return
        t = d.get("t")
        if t == "s":
            self._state(p, d)
        elif t == "act":
            self._act(p, d)
        elif t == "ping":
            ts = d.get("ts")
            ts = ts if isinstance(ts, (int, float)) and not isinstance(ts, bool) and math.isfinite(ts) else 0
            p.send(dumps({"t": "pong", "ts": ts, "online_count": self.online_count}))
        elif t == "join":
            self._join(p, d)

    def _join(self, p: Player, d: dict):
        p.name = clean_text(d.get("name")) or "Người chơi"
        p.cls = clean_key(d.get("cls"), "sword")
        p.race = clean_key(d.get("race"), "")
        self._leave(p)
        room = self._assign(d.get("room"))
        others = []
        if room != LOBBY:
            members = self.rooms.setdefault(room, set())
            others = [q.public() for q in members if q.has_state]
            members.add(p)
        p.room = room
        p.send(dumps({"t": "joined", "room": room, "players": others, "online_count": self.online_count}))

    def _state(self, p: Player, d: dict):
        if p.room == LOBBY:
            return
        p.x = round(num(d.get("x"), -200, WORLD_MAX, p.x), 1)
        p.y = round(num(d.get("y"), -200, WORLD_MAX, p.y), 1)
        p.f = round(num(d.get("f"), -10, 10, p.f), 2)
        p.mhp = int(num(d.get("mhp"), 1, 1_000_000, p.mhp))
        p.hp = int(num(d.get("hp"), 0, p.mhp, p.hp))
        p.al = 1 if d.get("al", 1) else 0
        if not p.has_state:                            # lần đầu: báo cho người khác biết có người mới
            p.has_state = True
            self._broadcast(p.room, dumps({"t": "join", "p": p.public()}), exclude=p)
            return
        self._broadcast(p.room, dumps({"t": "s", "id": p.id, "x": p.x, "y": p.y, "f": p.f,
                                       "hp": p.hp, "mhp": p.mhp, "al": p.al}), exclude=p)

    def _act(self, p: Player, d: dict):
        k = d.get("k")
        if p.room == LOBBY or not p.has_state or k not in ACTIONS:
            return
        a = round(num(d.get("a"), -10, 10, p.f), 2)
        self._broadcast(p.room, dumps({"t": "act", "id": p.id, "k": k, "a": a}), exclude=p)


# ----------------------------------------------------------------------------
# HTTP + WebSocket (aiohttp)
# ----------------------------------------------------------------------------
async def _writer(p: Player):
    """Task gửi dữ liệu ra socket; client chậm chỉ làm chậm chính nó, không chặn người khác."""
    try:
        while True:
            msg = await p.outbox.get()
            await p.ws.send_str(msg)
    except asyncio.CancelledError:
        raise
    except Exception:
        try:
            await p.ws.close()
        except Exception:
            pass


async def ws_handler(request: web.Request):
    if ALLOWED_ORIGINS and request.headers.get("Origin") not in ALLOWED_ORIGINS:
        return web.Response(status=403, text="origin not allowed")
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=4096)
    await ws.prepare(request)
    hub: Hub = request.app["hub"]
    p = Player(ws)
    writer = asyncio.create_task(_writer(p))
    hub.add(p)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                hub.on_message(p, msg.data)
            elif msg.type == WSMsgType.ERROR:
                break
    finally:
        hub.remove(p)
        writer.cancel()
    return ws


async def index(request: web.Request):
    if not HTML_PATH.exists():
        return web.Response(status=404, text=f"Không tìm thấy {HTML_PATH}. Đặt file game cạnh server.py hoặc set GAME_HTML.")
    return web.FileResponse(HTML_PATH)


async def healthz(request: web.Request):
    return web.json_response({"ok": True, "online_count": request.app["hub"].online_count})


async def _background(app: web.Application):
    async def online_loop():
        while True:
            await asyncio.sleep(1)
            app["hub"].flush_online()
    task = asyncio.create_task(online_loop())
    yield
    task.cancel()


def create_app() -> web.Application:
    app = web.Application()
    app["hub"] = Hub()
    app.add_routes([web.get("/", index), web.get("/ws", ws_handler), web.get("/healthz", healthz)])
    app.cleanup_ctx.append(_background)
    return app


if __name__ == "__main__":
    print(f"Game: http://localhost:{PORT}   WebSocket: /ws   (file: {HTML_PATH})")
    web.run_app(create_app(), host=HOST, port=PORT)
