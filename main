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
