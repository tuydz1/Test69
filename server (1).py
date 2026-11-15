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

PHÒNG PvP TUỲ CHỈNH (mới) — server chỉ quản lý phòng + chuyển tiếp gói tin, chủ phòng (host) chạy mô phỏng trận:
  Client -> Server
    {"t":"prof","name":"..","cls":"sword","race":"nhan","rk":123}   cập nhật hồ sơ (tên hiển thị, hạng)
    {"t":"chat","s":"w"|"r","m":"nội dung"}                         chat thế giới / chat phòng
    {"t":"rlist"}                                                    xin danh sách phòng công khai
    {"t":"board"}                                                    xin bảng hạng người chơi đang online
    {"t":"rc","mode":"dm","size":8,"bots":0,"fill":0,"fair":1,"priv":0}   tạo phòng (người tạo là chủ phòng)
    {"t":"rj","code":"AB12"}                                         vào phòng bằng mã
    {"t":"rl"}                                                       rời phòng
    {"t":"rcfg", ...}                                                (chủ phòng) đổi mode/size/bots/fill/fair/priv
    {"t":"rrdy","v":1,"ld":{"cls":..,"race":..,"pet":..,"eq":[..]}}  sẵn sàng + trang bị mang vào trận
    {"t":"rtm","tm":0|1}                                             chọn đội (chế độ đội)
    {"t":"rk","id":".."}                                             (chủ phòng) đuổi người
    {"t":"rst"}                                                      (chủ phòng) bắt đầu trận
    {"t":"mst",...} {"t":"snap",...} {"t":"res",...} {"t":"mend"}   (chủ phòng) khởi động trận / trạng thái / kết quả / kết thúc
    {"t":"in",...}                                                   (người chơi thường) input gửi cho chủ phòng
  Server -> Client
    {"t":"room",...}  trạng thái phòng   {"t":"rlist","rooms":[..]}   {"t":"rgo",...} (chỉ host) lệnh dựng trận
    {"t":"rclose","reason":".."}  phòng đóng / bị đuổi   {"t":"rleft","id":".."} (chỉ host) có người rời giữa trận
    {"t":"rerr","m":".."}  lỗi   {"t":"chat",...}  tin nhắn   {"t":"board","list":[..]}

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
import random
import re
import time
import traceback
import uuid
from collections import deque
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
RATE_PER_SEC = 100.0                                   # giới hạn gói tin / giây / client (host gửi snapshot ~20/s + sự kiện)
RATE_BURST = 200.0
OUTBOX_MAX = 256                                       # hàng đợi gửi / client (đầy -> bỏ gói cũ nhất)
MSG_MAX = 4096                                         # kích thước tối đa gói thường (byte)
SNAP_MAX = 262144                                      # kích thước tối đa gói của host (snapshot / khởi động trận / kết quả)
PLAYER_NAME_MAX = 20
CHAT_MAX = 100
CHAT_GAP = 0.7                                         # giây tối thiểu giữa 2 tin chat
START_TIMEOUT = 20.0                                   # giây chờ host dựng trận trước khi huỷ trạng thái "starting"
ROOM_LIST_MAX = 30
CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"        # mã phòng (bỏ ký tự dễ nhầm I/O/0/1)

# Chế độ PvP tuỳ chỉnh: min/max = tổng số chiến binh (người + bot), kind: ffa (hỗn chiến) | team (2 đội) | coop (cùng phe đánh quái)
MODE_CFG = {
    "dm":      {"min": 2, "max": 8, "kind": "ffa"},
    "crystal": {"min": 2, "max": 8, "kind": "ffa"},
    "bomb":    {"min": 2, "max": 8, "kind": "ffa"},
    "chaos":   {"min": 2, "max": 8, "kind": "ffa"},
    "br":      {"min": 2, "max": 8, "kind": "ffa"},
    "duel":    {"min": 2, "max": 2, "kind": "team"},
    "duo":     {"min": 4, "max": 4, "kind": "team"},
    "team":    {"min": 2, "max": 8, "kind": "team"},
    "king":    {"min": 2, "max": 8, "kind": "team"},
    "moba1":   {"min": 2, "max": 2, "kind": "team"},
    "moba3":   {"min": 2, "max": 8, "kind": "team"},
    "moba5":   {"min": 2, "max": 8, "kind": "team"},
    "boss":    {"min": 1, "max": 8, "kind": "coop"},
    "horde":   {"min": 1, "max": 8, "kind": "coop"},
    "endless": {"min": 1, "max": 8, "kind": "coop"},
}

ROOM_RE = re.compile(r"^[a-z0-9_]{1,16}$")
KEY_RE = re.compile(r"^[a-z0-9_]{1,16}$")
CTRL_RE = re.compile(r"[\x00-\x1f\x7f<>]")
CODE_RE = re.compile(r"^[A-Z0-9]{4}$")


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
                 "x", "y", "f", "hp", "mhp", "al", "tokens", "t_last",
                 "rm", "rdy", "team", "ld", "rk", "t_chat")

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
        self.rm = None                                 # phòng PvP tuỳ chỉnh đang ở (Room) hoặc None
        self.rdy = False                               # đã bấm sẵn sàng chưa
        self.team = 0                                  # đội trong phòng (chế độ đội)
        self.ld = {}                                   # trang bị mang vào trận: {"pet":..,"eq":[..]}
        self.rk = 0                                    # điểm hạng (do client báo, chỉ để hiển thị bảng hạng)
        self.t_chat = 0.0

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
# Phòng PvP tuỳ chỉnh (có mã phòng, chủ phòng, số người / bot, đội)
# ----------------------------------------------------------------------------
def to_int(v, lo, hi, default) -> int:
    return int(num(v, lo, hi, default))


def clean_ld(d) -> dict:
    """Trang bị mang vào trận (chỉ nhận khoá hợp lệ; client chủ phòng tự đối chiếu với bảng dữ liệu game)."""
    out = {}
    if not isinstance(d, dict):
        return out
    pet = d.get("pet")
    if isinstance(pet, str) and KEY_RE.match(pet):
        out["pet"] = pet
    eq = d.get("eq")
    if isinstance(eq, list):
        out["eq"] = [e for e in eq[:6] if isinstance(e, str) and KEY_RE.match(e)]
    out["lv"] = to_int(d.get("lv"), 1, 15, 1)          # cấp cơ giáp đang dùng
    out["pl"] = to_int(d.get("pl"), 1, 10, 1)          # cấp linh sủng đang dùng
    sk = d.get("sk")
    if isinstance(sk, list) and len(sk) == 3:
        out["sk"] = [to_int(x, 1, 5, 1) for x in sk]   # cấp 3 kỹ năng (s1, s2, ult)
    return out


class Room:
    """Server chỉ giữ cấu hình + danh sách thành viên; chủ phòng (host) chạy mô phỏng cả trận."""
    __slots__ = ("code", "host", "members", "mode", "size", "bots", "fill", "fair", "priv", "state", "t_state")

    def __init__(self, code: str, host: Player, mode: str):
        self.code = code
        self.host = host
        self.members = [host]                          # theo thứ tự vào phòng
        self.mode = mode
        self.size = MODE_CFG[mode]["max"]              # tổng số chiến binh (người + bot)
        self.bots = 0                                  # số bot muốn thêm
        self.fill = False                              # tự điền bot vào chỗ trống khi bắt đầu
        self.fair = True                               # chuẩn hoá chỉ số (không dùng trang bị / linh sủng)
        self.priv = False                              # phòng riêng: không hiện trong danh sách
        self.state = "waiting"                         # waiting | starting | playing
        self.t_state = time.monotonic()

    @property
    def kind(self) -> str:
        return MODE_CFG[self.mode]["kind"]

    @property
    def cap(self) -> int:                              # số chỗ cho người thật
        return max(1, self.size - self.bots)


# ----------------------------------------------------------------------------
# Hub: quản lý phòng + xử lý gói tin (không phụ thuộc aiohttp nên dễ test)
# ----------------------------------------------------------------------------
class Hub:
    def __init__(self, max_room: int = MAX_ROOM):
        self.players = {}          # id -> Player
        self.rooms = {}            # tên phòng -> set(Player)   (không chứa sảnh)
        self.max_room = max_room
        self._online_dirty = False
        self.croom = {}            # mã phòng -> Room (phòng PvP tuỳ chỉnh)
        self.max_croom = 200
        self.hist = deque(maxlen=20)   # 20 tin chat thế giới gần nhất (gửi cho người mới vào)

    @property
    def online_count(self) -> int:
        return len(self.players)

    # --- vòng đời kết nối ---
    def add(self, p: Player):
        self.players[p.id] = p
        p.send(dumps({"t": "welcome", "id": p.id, "online_count": self.online_count, "hist": list(self.hist)}))
        self._online_dirty = True

    def remove(self, p: Player):
        self._rleave(p)
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
        n = len(raw)
        rm = p.rm
        if rm is not None and rm.host is p and rm.state == "playing" and n <= SNAP_MAX and raw.startswith('{"t":"snap"'):
            for q in rm.members:                       # snapshot của host: chuyển tiếp nguyên văn, không cần parse
                if q is not p:
                    q.send(raw)
            return
        if n > SNAP_MAX:
            return
        try:
            d = json.loads(raw)
        except (ValueError, TypeError):
            return
        if not isinstance(d, dict):
            return
        t = d.get("t")
        if n > MSG_MAX and not (t in ("mst", "res") and rm is not None and rm.host is p):
            return
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
        elif t == "in":                                # input của người chơi thường -> chuyển cho host
            if rm is not None and rm.state == "playing" and rm.host is not p:
                d["id"] = p.id
                rm.host.send(dumps(d))
        elif t == "prof":
            self._prof(p, d)
        elif t == "chat":
            self._chat(p, d)
        elif t == "rlist":
            self._rlist(p)
        elif t == "board":
            self._board(p)
        elif t == "rc":
            self._rcreate(p, d)
        elif t == "rj":
            self._rjoin(p, d)
        elif t == "rl":
            self._rleave(p)
        elif t == "rcfg":
            self._rcfg(p, d)
        elif t == "rrdy":
            self._rready(p, d)
        elif t == "rtm":
            self._rteam(p, d)
        elif t == "rk":
            self._rkick(p, d)
        elif t == "rst":
            self._rstart(p)
        elif t == "mst":
            self._rmst(p, d, raw)
        elif t == "res":
            if rm is not None and rm.host is p and rm.state == "playing":
                for q in rm.members:
                    if q is not p:
                        q.send(raw)
        elif t == "mend":
            self._rmend(p)

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

    # ------------------------------------------------------------------
    # Hồ sơ, chat, bảng hạng
    # ------------------------------------------------------------------
    def _prof(self, p: Player, d: dict):
        n = clean_text(d.get("name"), PLAYER_NAME_MAX)
        if n:
            p.name = n
        p.cls = clean_key(d.get("cls"), p.cls)
        p.race = clean_key(d.get("race"), p.race)
        p.rk = to_int(d.get("rk"), 0, 1_000_000, p.rk)
        room = p.rm
        if room is not None and room.state == "waiting":
            self._uniq_name(room, p)
            self._room_sync(room)

    def _chat(self, p: Player, d: dict):
        now = time.monotonic()
        if now - p.t_chat < CHAT_GAP:
            return
        m = clean_text(d.get("m"), CHAT_MAX)
        if not m:
            return
        p.t_chat = now
        scope = "r" if d.get("s") == "r" and p.rm is not None else "w"
        msg = dumps({"t": "chat", "s": scope, "id": p.id, "n": p.name, "m": m})
        if scope == "r":
            for q in p.rm.members:
                q.send(msg)
        else:
            self.hist.append({"n": p.name, "m": m})
            for q in self.players.values():
                if q.rm is None or q.rm.state != "playing":     # người đang trong trận không bị spam chat thế giới
                    q.send(msg)

    def _board(self, p: Player):
        lst = sorted(self.players.values(), key=lambda q: -q.rk)[:15]
        p.send(dumps({"t": "board", "online": self.online_count,
                      "list": [{"id": q.id, "n": q.name, "rk": q.rk} for q in lst]}))

    # ------------------------------------------------------------------
    # Phòng PvP tuỳ chỉnh
    # ------------------------------------------------------------------
    def _err(self, p: Player, m: str):
        p.send(dumps({"t": "rerr", "m": m}))

    def _new_code(self):
        for _ in range(60):
            c = "".join(random.choice(CODE_CHARS) for _ in range(4))
            if c not in self.croom:
                return c
        return None

    def _team_caps(self, room: Room):
        if room.kind == "team":
            return [(room.size + 1) // 2, room.size // 2]
        return [room.size, 0]

    def _team_count(self, room: Room, t: int, exclude=None) -> int:
        return sum(1 for m in room.members if m is not exclude and m.team == t)

    def _pick_team(self, room: Room, p: Player) -> int:
        if room.kind != "team":
            return 0
        caps = self._team_caps(room)
        c0, c1 = self._team_count(room, 0, p), self._team_count(room, 1, p)
        if c0 <= c1 and c0 < caps[0]:
            return 0
        if c1 < caps[1]:
            return 1
        return 0 if c0 < caps[0] else 1

    def _rebalance(self, room: Room):
        if room.kind != "team":
            for m in room.members:
                m.team = 0
            return
        caps = self._team_caps(room)
        for t in (0, 1):
            mine = [m for m in room.members if m.team == t]
            while len(mine) > caps[t]:
                mine.pop().team = 1 - t

    def _uniq_name(self, room: Room, p: Player):
        """Hai người cùng tên trong 1 phòng -> thêm hậu tố #2, #3… để phân biệt."""
        names = {m.name.lower() for m in room.members if m is not p}
        if p.name.lower() in names:
            base = p.name[:PLAYER_NAME_MAX - 4]
            for i in range(2, 10):
                cand = "%s #%d" % (base, i)
                if cand.lower() not in names:
                    p.name = cand
                    break

    def _room_msg(self, room: Room, p: Player) -> str:
        return dumps({
            "t": "room", "c": room.code, "host": room.host.id, "you": p.id, "mode": room.mode,
            "size": room.size, "bots": room.bots, "cap": room.cap, "fill": int(room.fill),
            "fair": int(room.fair), "priv": int(room.priv), "st": room.state,
            "mem": [{"id": m.id, "n": m.name, "cls": m.cls, "race": m.race, "tm": m.team,
                     "rd": int(m.rdy or m is room.host), "rk": m.rk} for m in room.members]})

    def _room_sync(self, room: Room):
        for m in room.members:
            m.send(self._room_msg(room, m))

    def _rbrief(self, room: Room) -> dict:
        return {"c": room.code, "mode": room.mode, "size": room.size, "bots": room.bots, "h": len(room.members),
                "cap": room.cap, "st": room.state, "ho": room.host.name, "fair": int(room.fair)}

    def _rlist(self, p: Player):
        rooms = [self._rbrief(r) for r in self.croom.values() if not r.priv]
        rooms.sort(key=lambda r: (r["st"] != "waiting", -r["h"]))
        p.send(dumps({"t": "rlist", "online": self.online_count, "rooms": rooms[:ROOM_LIST_MAX]}))

    def _apply_cfg(self, room: Room, d: dict):
        """Áp cấu hình phòng (nguyên tử). Trả về chuỗi lỗi nếu không hợp lệ, None nếu OK."""
        mode = d.get("mode", room.mode)
        if not isinstance(mode, str) or mode not in MODE_CFG:
            return "Chế độ không hợp lệ"
        cfg = MODE_CFG[mode]
        size = to_int(d.get("size", room.size), cfg["min"], cfg["max"], room.size)
        size = max(cfg["min"], min(cfg["max"], size))
        bots = to_int(d.get("bots", room.bots), 0, max(0, size - 1), 0)
        if size - bots < len(room.members):
            return "Phòng đang có %d người, không thể giảm số chỗ" % len(room.members)
        changed = (mode, size, bots) != (room.mode, room.size, room.bots)
        old_kind = room.kind
        room.mode, room.size, room.bots = mode, size, bots
        for key in ("fill", "fair", "priv"):
            if key in d:
                v = bool(d.get(key))
                if getattr(room, key) != v:
                    changed = True
                setattr(room, key, v)
        if room.kind == "team" and old_kind != "team":  # vừa chuyển sang chế độ đội -> chia đều người vào 2 đội
            caps, cnt = self._team_caps(room), [0, 0]
            for m in room.members:
                t = 0 if cnt[0] <= cnt[1] else 1
                if cnt[t] >= caps[t]:
                    t = 1 - t
                m.team = t
                cnt[t] += 1
        self._rebalance(room)
        if changed:                                    # đổi cấu hình -> mọi người phải xác nhận sẵn sàng lại
            for m in room.members:
                if m is not room.host:
                    m.rdy = False
        return None

    def _rcreate(self, p: Player, d: dict):
        if p.rm is not None:
            return self._err(p, "Bạn đang ở trong một phòng")
        if len(self.croom) >= self.max_croom:
            return self._err(p, "Máy chủ đang đầy phòng, thử lại sau")
        code = self._new_code()
        if code is None:
            return self._err(p, "Máy chủ đang bận, thử lại sau")
        mode = d.get("mode") if isinstance(d.get("mode"), str) and d.get("mode") in MODE_CFG else "dm"
        room = Room(code, p, mode)
        p.rm, p.rdy, p.team = room, False, 0
        self.croom[code] = room
        self._apply_cfg(room, d)                       # lỗi (nếu có) bỏ qua: giữ mặc định
        if "ld" in d:
            self._set_ld(p, d["ld"])
        self._room_sync(room)

    def _rjoin(self, p: Player, d: dict):
        if p.rm is not None:
            return self._err(p, "Bạn đang ở trong một phòng")
        code = str(d.get("code") or "").strip().upper()
        room = self.croom.get(code) if CODE_RE.match(code) else None
        if room is None:
            return self._err(p, "Không tìm thấy phòng " + code[:6])
        if room.state != "waiting":
            return self._err(p, "Phòng đang trong trận")
        if len(room.members) >= room.cap:
            return self._err(p, "Phòng đã đầy")
        p.rm, p.rdy = room, False
        p.team = self._pick_team(room, p)
        room.members.append(p)
        self._uniq_name(room, p)
        self._room_sync(room)

    def _rleave(self, p: Player, reason=None):
        room = p.rm
        if room is None:
            return
        p.rm, p.rdy = None, False
        if p in room.members:
            room.members.remove(p)
        if reason:
            p.send(dumps({"t": "rclose", "reason": reason}))
        if not room.members:
            self.croom.pop(room.code, None)
            return
        if room.host is p:
            if room.state in ("starting", "playing"):  # host rời giữa trận -> đóng phòng (chưa hỗ trợ chuyển host)
                for m in list(room.members):
                    m.rm, m.rdy = None, False
                    m.send(dumps({"t": "rclose", "reason": "host_left"}))
                self.croom.pop(room.code, None)
                return
            room.host = room.members[0]
            room.host.rdy = False
        elif room.state == "playing":
            room.host.send(dumps({"t": "rleft", "id": p.id}))
        self._room_sync(room)

    def _set_ld(self, p: Player, ld):
        if not isinstance(ld, dict):
            return
        p.cls = clean_key(ld.get("cls"), p.cls)
        p.race = clean_key(ld.get("race"), p.race)
        p.ld = clean_ld(ld)

    def _rcfg(self, p: Player, d: dict):
        room = p.rm
        if room is None or room.host is not p or room.state != "waiting":
            return
        err = self._apply_cfg(room, d)
        if err:
            self._err(p, err)
        self._room_sync(room)

    def _rready(self, p: Player, d: dict):
        room = p.rm
        if room is None or room.state != "waiting":
            return
        if "v" in d:
            p.rdy = bool(d.get("v"))
        if "ld" in d:
            self._set_ld(p, d["ld"])
        self._room_sync(room)

    def _rteam(self, p: Player, d: dict):
        room = p.rm
        if room is None or room.state != "waiting" or room.kind != "team":
            return
        t = 1 if d.get("tm") == 1 else 0
        if self._team_count(room, t, p) >= self._team_caps(room)[t]:
            return self._err(p, "Đội này đã đầy")
        p.team = t
        self._room_sync(room)

    def _rkick(self, p: Player, d: dict):
        room = p.rm
        if room is None or room.host is not p or room.state != "waiting":
            return
        for m in list(room.members):
            if m is not p and m.id == d.get("id"):
                self._rleave(m, "kicked")
                return

    def _plan(self, room: Room):
        """Số bot cần thêm theo từng đội (chế độ đội) hoặc tổng (hỗn chiến / co-op)."""
        humans = len(room.members)
        if room.kind == "team":
            h = [self._team_count(room, 0), self._team_count(room, 1)]
            if room.fill:
                caps = self._team_caps(room)
                return [max(0, caps[0] - h[0]), max(0, caps[1] - h[1])]
            b = [0, 0]
            for _ in range(room.bots):
                i = 0 if h[0] + b[0] <= h[1] + b[1] else 1
                b[i] += 1
            return b
        return [max(0, room.size - humans) if room.fill else room.bots]

    def _rstart(self, p: Player):
        room = p.rm
        if room is None or room.host is not p or room.state != "waiting":
            return
        wait = [m.name for m in room.members if m is not p and not m.rdy]
        if wait:
            return self._err(p, "Chưa sẵn sàng: " + ", ".join(wait[:3]))
        plan = self._plan(room)
        total = len(room.members) + sum(plan)
        need = MODE_CFG[room.mode]["min"]
        if total < need:
            return self._err(p, "Cần tối thiểu %d chiến binh (đang có %d). Thêm bot hoặc bật 'Điền bot'" % (need, total))
        if room.kind == "team":
            h = [self._team_count(room, 0), self._team_count(room, 1)]
            if h[0] + plan[0] < 1 or h[1] + plan[1] < 1:
                return self._err(p, "Mỗi đội cần ít nhất 1 chiến binh")
        room.state, room.t_state = "starting", time.monotonic()
        mem = [{"id": m.id, "n": m.name, "cls": m.cls, "race": m.race, "tm": m.team, "ld": m.ld} for m in room.members]
        p.send(dumps({"t": "rgo", "mode": room.mode, "kind": room.kind, "size": room.size,
                      "fair": int(room.fair), "bots": plan, "mem": mem}))
        self._room_sync(room)

    def _rmst(self, p: Player, d: dict, raw: str):
        """Host đã dựng xong trận -> chuyển nguyên văn cho mọi người, chuyển phòng sang 'playing'."""
        room = p.rm
        if room is None or room.host is not p or room.state != "starting":
            return
        room.state, room.t_state = "playing", time.monotonic()
        ids = {m.id for m in room.members}
        pl = d.get("pl")
        for e in pl if isinstance(pl, list) else []:
            if isinstance(e, dict) and e.get("id") not in ids and e.get("id") != p.id:
                p.send(dumps({"t": "rleft", "id": e.get("id")}))   # rời giữa lúc đang dựng trận
        for m in room.members:
            if m is not p:
                m.send(raw)
        self._room_sync(room)

    def _rmend(self, p: Player):
        room = p.rm
        if room is None or room.host is not p or room.state not in ("starting", "playing"):
            return
        room.state, room.t_state = "waiting", time.monotonic()
        for m in room.members:
            if m is not p:
                m.rdy = False
        self._room_sync(room)

    def tick(self):
        """Gọi mỗi ~1 giây: huỷ trạng thái 'starting' nếu host không dựng xong trận."""
        now = time.monotonic()
        for room in list(self.croom.values()):
            if room.state == "starting" and now - room.t_state > START_TIMEOUT:
                room.state, room.t_state = "waiting", now
                self._err(room.host, "Dựng trận quá lâu, đã huỷ. Hãy thử lại")
                self._room_sync(room)


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
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=SNAP_MAX)
    await ws.prepare(request)
    hub: Hub = request.app["hub"]
    p = Player(ws)
    writer = asyncio.create_task(_writer(p))
    hub.add(p)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    hub.on_message(p, msg.data)
                except Exception:                      # 1 gói lỗi không được làm đứt kết nối / sập server
                    traceback.print_exc()
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
    return web.json_response({"ok": True, "online_count": request.app["hub"].online_count,
                              "rooms": len(request.app["hub"].croom)})


async def _background(app: web.Application):
    async def online_loop():
        while True:
            await asyncio.sleep(1)
            app["hub"].flush_online()
            app["hub"].tick()
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
