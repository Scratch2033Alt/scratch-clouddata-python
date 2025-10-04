#!/usr/bin/env python3
import asyncio
import argparse
import json
import time
import psutil
import ssl
import aiosqlite
from aiohttp import web, WSMsgType, ClientSession
import os

# ---------------- Global Config ----------------
projects = {}
logs = {}
bot_logs = {}
db = None
use_db = False
save_file_path = None

UA_STRING = "ScratchWarpCloud/1.0 (https://github.com/Scratch2033Alt OR https://scratch.mit.edu/users/wetogeter)"

SCRATCH_SESSIONID = None
SCRATCH_CSRF = None
SCRATCH_USER = None

connections = {}
ws_to_project = {}
scratch_sync_task = None
scratch_sync_queue = asyncio.Queue()
scratch_sync_running = False

BROWSER_KEYWORDS = ["Chrome", "Firefox", "Safari", "Edge"]

# ---------------- Memory Monitor ----------------
def memory_usage_high():
    return psutil.virtual_memory().percent >= 75

async def flush_memory_to_db():
    global projects, logs, bot_logs, db
    if not db:
        return
    print("[FLUSH] Memory high, flushing to DB...")

    await db.execute("BEGIN")
    for project_id, vars_dict in list(projects.items()):
        for name, value in vars_dict.items():
            await db.execute(
                "INSERT OR REPLACE INTO projects (project_id, name, value) VALUES (?, ?, ?)",
                (project_id, name, value),
            )
    for project_id, entries in list(logs.items()):
        for entry in entries:
            await db.execute(
                "INSERT INTO logs (project_id, user, verb, name, value, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    project_id,
                    entry["user"],
                    entry["verb"],
                    entry["name"],
                    entry["value"],
                    entry["timestamp"],
                ),
            )
    for project_id, entries in list(bot_logs.items()):
        for entry in entries:
            await db.execute(
                "INSERT INTO bot_logs (project_id, user, verb, name, value, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    project_id,
                    entry["user"],
                    entry["verb"],
                    entry["name"],
                    entry["value"],
                    entry["timestamp"],
                ),
            )
    await db.commit()
    projects.clear()
    logs.clear()
    bot_logs.clear()
    print("[FLUSH] Completed.")

# ---------------- DB Setup ----------------
async def init_db(path):
    global db
    db = await aiosqlite.connect(path)
    await db.execute(
        """CREATE TABLE IF NOT EXISTS projects (
            project_id TEXT,
            name TEXT,
            value TEXT,
            PRIMARY KEY (project_id, name)
        )"""
    )
    await db.execute(
        """CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id TEXT,
            user TEXT,
            verb TEXT,
            name TEXT,
            value TEXT,
            timestamp INTEGER
        )"""
    )
    await db.execute(
        """CREATE TABLE IF NOT EXISTS bot_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id TEXT,
            user TEXT,
            verb TEXT,
            name TEXT,
            value TEXT,
            timestamp INTEGER
        )"""
    )
    await db.commit()

# ---------------- Save/Load JSON ----------------
def save_to_file():
    if not save_file_path:
        return
    data = {"projects": projects, "logs": logs, "bot_logs": bot_logs}
    with open(save_file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

def load_from_file():
    if save_file_path and os.path.exists(save_file_path):
        with open(save_file_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                projects.update(data.get("projects", {}))
                logs.update(data.get("logs", {}))
                bot_logs.update(data.get("bot_logs", {}))
                print(f"[SAVE-FILE] Loaded {len(projects)} projects from {save_file_path}")
            except Exception as e:
                print(f"[SAVE-FILE] Error loading: {e}")

# ---------------- Logging helpers ----------------
async def add_log(project_id, user, verb, name, value, bot=False):
    entry = {
        "user": user,
        "verb": verb,
        "name": name,
        "value": value,
        "timestamp": int(time.time() * 1000),
    }
    target = bot_logs if bot else logs

    if memory_usage_high():
        await flush_memory_to_db()
        if db:
            table = "bot_logs" if bot else "logs"
            await db.execute(
                f"INSERT INTO {table} (project_id, user, verb, name, value, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                (project_id, user, verb, name, value, entry["timestamp"]),
            )
            await db.commit()
    else:
        target.setdefault(project_id, []).append(entry)
        if db:
            table = "bot_logs" if bot else "logs"
            await db.execute(
                f"INSERT INTO {table} (project_id, user, verb, name, value, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                (project_id, user, verb, name, value, entry["timestamp"]),
            )
            await db.commit()
        elif save_file_path and not args.sync:
            save_to_file()

    print(f"[LOG{'-BOT' if bot else ''}] {project_id} {user} {verb} {name}={value}")
    return entry

# ---------------- Scratch login ----------------
async def scratch_login(username, password):
    global SCRATCH_SESSIONID, SCRATCH_CSRF, SCRATCH_USER
    SCRATCH_USER = username
    login_url = "https://scratch.mit.edu/accounts/login/"
    headers = {
        "Cookie": "scratchcsrftoken=a; scratchlanguage=en",
        "Origin": "https://scratch.mit.edu",
        "Referer": "https://scratch.mit.edu/",
        "X-CSRFToken": "a",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json",
        "User-Agent": UA_STRING,
    }
    payload = {"useMessages": True, "username": username, "password": password}
    async with ClientSession(headers=headers) as session:
        async with session.post(login_url, json=payload) as resp:
            if resp.status != 200:
                raise Exception("Scratch login failed")
            cookies = session.cookie_jar.filter_cookies("https://scratch.mit.edu")
            SCRATCH_SESSIONID = cookies.get("scratchsessionsid").value
            SCRATCH_CSRF = cookies.get("scratchcsrftoken").value
            print(f"[SCRATCH LOGIN] {username} logged in.")

# ---------------- Auth check ----------------
def is_browser_like(ua: str):
    return any(k in ua for k in BROWSER_KEYWORDS)

async def validate_user_exists(user: str):
    url = f"https://scratch.mit.edu/users/{user}"
    async with ClientSession(headers={"User-Agent": UA_STRING}) as s:
        async with s.get(url) as resp:
            return resp.status == 200

async def validate_session(sessionid: str, csrftoken: str):
    url = "https://api.scratch.mit.edu/proxy/comments/project/1219579420"
    headers = {
        "Cookie": f"scratchsessionsid={sessionid}; scratchcsrftoken={csrftoken}; scratchlanguage=en",
        "Origin": "https://scratch.mit.edu",
        "Referer": "https://scratch.mit.edu/",
        "X-CSRFToken": csrftoken,
        "Content-Type": "application/json",
        "User-Agent": UA_STRING,
    }
    payload = {"comment": "auth test"}
    async with ClientSession(headers=headers) as s:
        async with s.post(url, json=payload) as resp:
            return resp.status == 200

# ---------------- Validation ----------------
def validate_value(value: str):
    if len(value) > 1000:
        return False
    try:
        float(value)
    except Exception:
        return False
    return True

# ---------------- Scratch Sync Manager ----------------
async def scratch_sync_loop():
    global scratch_sync_running
    scratch_sync_running = True
    uri = "wss://clouddata.scratch.mit.edu"
    headers = {"User-Agent": UA_STRING}
    if SCRATCH_SESSIONID and SCRATCH_CSRF:
        headers["Cookie"] = f"scratchsessionsid={SCRATCH_SESSIONID}; scratchcsrftoken={SCRATCH_CSRF}; scratchlanguage=en"

    while scratch_sync_running:
        try:
            async with ClientSession(headers=headers) as session:
                async with session.ws_connect(uri) as ws:
                    print("[SYNC] Connected to Scratch")
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if data.get("method") == "set":
                                pid = str(data.get("project_id"))
                                name = data.get("name")
                                value = data.get("value")
                                if not validate_value(value):
                                    continue
                                projects.setdefault(pid, {})[name] = value
                                await add_log(pid, "ScratchSync", "set_var", name, value)
                                for peer in connections.get(pid, set()):
                                    try:
                                        await peer.send_json({"method":"set","name":name,"value":value,"project_id":pid})
                                    except: pass
        except Exception as e:
            print(f"[SYNC] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)

# ---------------- WebSocket Handler ----------------
async def ws_handler(request):
    app_args = request.app["args"]
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    ws_id = id(ws)
    ua = request.headers.get("User-Agent", "")
    browser_like = is_browser_like(ua)

    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            data = json.loads(msg.data)
            method = data.get("method")
            project_id = str(data.get("project_id", ""))
            user = data.get("user", "UNKNOWN")

            if method == "handshake":
                if app_args.use_authorization and browser_like:
                    if not await validate_user_exists(user):
                        await ws.send_json({"Error":"Sorry! Banned accounts cannot use the cloud.","ScratchAuthFailed":"True"})
                        await add_log(project_id, user, "auth_fail", "handshake", "banned", bot=True)
                        await ws.close(); return ws
                    sid, csrf = data.get("sessionid"), data.get("csrftoken")
                    if not sid or not csrf or not await validate_session(sid, csrf):
                        await ws.send_json({"Error":"Sorry! Banned accounts cannot use the cloud.","ScratchAuthFailed":"True"})
                        await add_log(project_id, user, "auth_fail", "handshake", "bad_session", bot=True)
                        await ws.close(); return ws

                connections.setdefault(project_id, set()).add(ws)
                ws_to_project[ws_id] = project_id
                for name, value in projects.get(project_id, {}).items():
                    await ws.send_json({"method":"set","name":name,"value":value,"project_id":project_id})

            elif method == "set":
                name, value = data.get("name"), str(data.get("value"))
                if not validate_value(value):
                    await ws.send_json({"Error":"Invalid value. Must be numeric and <1000 chars."})
                    continue
                projects.setdefault(project_id, {})[name] = value
                await add_log(project_id, user, "set_var", name, value, bot=not browser_like)
                for peer in connections.get(project_id, set()):
                    if peer is not ws:
                        await peer.send_json({"method":"set","name":name,"value":value,"project_id":project_id})

    finally:
        pid = ws_to_project.pop(ws_id, None)
        if pid and ws in connections.get(pid, set()):
            connections[pid].discard(ws)
            if not connections[pid]: connections.pop(pid, None)
        await ws.close()
    return ws

# ---------------- Logs Endpoints ----------------
async def handle_logs(request): return web.json_response(logs.get(request.query.get("project",""), []))
async def handle_bots(request): return web.json_response(bot_logs.get(request.query.get("project",""), []))

# ---------------- Main ----------------
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--secure-host","--s",action="store_true")
    parser.add_argument("--cert"); parser.add_argument("--key")
    parser.add_argument("--save-db"); parser.add_argument("--save-file")
    parser.add_argument("--session",nargs=2,metavar=("USER","PASS"))
    parser.add_argument("--use-authorization","--uA",action="store_true")
    parser.add_argument("--sync",action="store_true")
    args = parser.parse_args()
    global save_file_path; save_file_path = args.save_file
    global scratch_sync_task

    if args.save_db: await init_db(args.save_db)
    if args.save_file: load_from_file()
    if args.session: u,p=args.session; await scratch_login(u,p)
    if args.sync: scratch_sync_task = asyncio.create_task(scratch_sync_loop())

    app = web.Application(); app["args"]=args
    app.router.add_get("/", ws_handler); app.router.add_get("/logs", handle_logs); app.router.add_get("/bots", handle_bots)
    runner = web.AppRunner(app); await runner.setup()

    ssl_ctx=None
    if args.secure_host:
        cert=args.cert or "ssl.crt"; key=args.key or "ssl.key"
        if os.path.exists(cert) and os.path.exists(key):
            ssl_ctx=ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_ctx.load_cert_chain(cert,key)
        else: raise Exception("TLS enabled but ssl.crt or ssl.key not found")

    site=web.TCPSite(runner,"0.0.0.0",8765,ssl_context=ssl_ctx); await site.start()
    print("Server running...")

    try: await asyncio.Future()
    except asyncio.CancelledError: pass
    finally:
        if save_file_path: save_to_file()
        if db: await db.close()

if __name__=="__main__": asyncio.run(main())
