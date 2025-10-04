#!/usr/bin/env python3
"""
Full clouddata proxy+sync server implementing:
- --save-db (libsql-client ; local file or remote http/ws)
- --db-token (optional)
- --save-file (JSON)
- --session USER PASS (Scratch login)
- --proxy-scratch / --ps
- --sync
- --use-authorization / --uA (client auth)
- --secure-host / --s (TLS; default ssl.crt + ssl.key)
- Value validation (numeric only, <1000 chars)
- Persistent libsql-client connection with offline queue (db_offline_queue.json)
- Bots vs browser logs (/bots vs /logs)
"""

import argparse
import asyncio
import json
import os
import ssl
import time
from typing import Any, Dict, List, Optional

import psutil
from aiohttp import ClientSession, WSMsgType, web

# libsql-client (single DB lib for local & remote)
import libsql_client

# ----------------- Configuration & Globals -----------------
UA_STRING = "ScratchWarpCloud/1.0 (https://github.com/Scratch2033Alt OR https://scratch.mit.edu/users/wetogeter)"
BROWSER_KEYWORDS = ["Chrome", "Firefox", "Safari", "Edge", "Opera", "Edg/"]  # Edg/ catches Edge UA variants
ERROR_HTML = "ERROR! CANNOT FIND ERROR.HTML! IF YOU ARE THE SITE OWNER, PLEASE ADD AN ERROR.HTML TO YOUR ROOT FOLDER OF THIS PYTHON SCRIPT."
if os.path.isfile("error.html"):
      with open("error.html", "r") as file:
         ERROR_HTML = file.read()

projects: Dict[str, Dict[str, str]] = {}   # project_id -> {name: value}
logs: Dict[str, List[Dict[str, Any]]] = {}  # normal logs (validated browser clients)
bot_logs: Dict[str, List[Dict[str, Any]]] = {}  # bot logs + auth failures

connections: Dict[str, set] = {}  # project_id -> set(WebSocketResponse)
ws_to_project: Dict[int, str] = {}  # ws id -> project_id (cleanup)

# Scratch session (for outbound proxy/sync)
SCRATCH_SESSIONID: Optional[str] = None
SCRATCH_CSRF: Optional[str] = None
SCRATCH_USER: Optional[str] = None

# DB layer
db_client = None  # libsql-client client instance
db_url: Optional[str] = None
db_token: Optional[str] = None
db_online = False
db_pending_queue: List[Dict[str, Any]] = []  # pending writes (in-memory)
DB_OFFLINE_QUEUE_FILE = "db_offline_queue.json"

# save-file path
save_file_path: Optional[str] = None

# sync task
scratch_sync_task = None

# scratch_sync queue (payloads to send to Scratch when --sync)
scratch_sync_queue: asyncio.Queue = asyncio.Queue()

# memory threshold
MEMORY_THRESHOLD_PERCENT = 75

# args container used in various places
args = None

# Serve error.html on plain HTTP GET /
async def handle_root(request):
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return await ws_handler(request)
    else:
        peer = request.remote or "unknown"
        print(f"[ROOT] {request.method} / from {peer}")
        return web.Response(status=200, text=ERROR_HTML, content_type="text/html")

# Catch-all 404 → serve error.html
async def handle_404(request):
    # Suppress favicon logs
    if request.path != "/favicon.ico":
        peer = request.remote or "unknown"
        print(f"[404] {request.method} {request.path} from {peer}")
    return web.Response(status=404, text=ERROR_HTML, content_type="text/html")

# ----------------- Utility helpers -----------------
def memory_usage_high() -> bool:
    return psutil.virtual_memory().percent >= MEMORY_THRESHOLD_PERCENT


def is_browser_like(ua: str) -> bool:
    if not ua:
        return False
    ua_lower = ua
    return any(keyword in ua_lower for keyword in BROWSER_KEYWORDS)


def validate_value(value: str) -> bool:
    """Value must be numeric and < 1000 characters."""
    if value is None:
        return False
    if len(value) > 1000:
        return False
    try:
        # Accept numeric strings; float() accepts ints and floats
        float(value)
    except Exception:
        return False
    return True


# ----------------- File persistence (save-file) -----------------
def load_from_file():
    global projects, logs, bot_logs, save_file_path
    if not save_file_path:
        return
    if os.path.exists(save_file_path):
        try:
            with open(save_file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            projects.update({str(k): v for k, v in data.get("projects", {}).items()})
            logs.update({str(k): v for k, v in data.get("logs", {}).items()})
            bot_logs.update({str(k): v for k, v in data.get("bot_logs", {}).items()})
            print(f"[SAVE-FILE] Loaded from {save_file_path}")
        except Exception as e:
            print(f"[SAVE-FILE] Error loading {save_file_path}: {e}")


def save_to_file():
    global save_file_path
    if not save_file_path:
        return
    try:
        tmp = {"projects": projects, "logs": logs, "bot_logs": bot_logs}
        with open(save_file_path, "w", encoding="utf-8") as f:
            json.dump(tmp, f)
        print(f"[SAVE-FILE] Saved to {save_file_path}")
    except Exception as e:
        print(f"[SAVE-FILE] Error saving to {save_file_path}: {e}")


# ----------------- DB (libsql-client) layer -----------------
async def db_connect(url: str, token: Optional[str] = None):
    """
    Create libsql-client connection. We support:
      - local files: "file:cloud.db" or "sql://cloud.db" (we convert)
      - remote: "libsql://..." or "wss://..." or "https://..."
    We use libsql_client.create_client(...) which supports file:, ws:, wss:, http(s):.
    If token is provided, pass as authToken in the configuration (create_client supports dict config).
    """
    global db_client, db_url, db_token, db_online
    db_url = url
    db_token = token
    print(f"[DB] Connecting to {url} (token={'yes' if token else 'no'})")
    try:
        # libsql_client.create_client accepts a string URL or a dict; to pass auth token we pass dict
        create_arg = {"url": url}
        if token:
            # many libsql client examples use 'authToken' or 'auth_token', the python client accepts dict
            create_arg["authToken"] = token
        # create_client returns an async context manager; we create persistent client by calling create_client and keeping it
        db_client = await libsql_client.create_client(create_arg) if isinstance(create_arg, dict) else await libsql_client.create_client(url)  # type: ignore
        # 'db_client' should now have an 'execute' coroutine method
        db_online = True
        # Ensure tables exist
        await db_execute(
            """CREATE TABLE IF NOT EXISTS projects (
                   project_id TEXT,
                   name TEXT,
                   value TEXT,
                   PRIMARY KEY (project_id, name)
               )"""
        )
        await db_execute(
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
        await db_execute(
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
        print("[DB] Connected and tables ensured")
    except Exception as e:
        db_client = None
        db_online = False
        print(f"[DB] Connection failed: {e}")


async def db_close():
    global db_client, db_online
    if db_client is not None:
        try:
            await db_client.close()
        except Exception:
            pass
    db_client = None
    db_online = False


async def db_execute(sql: str, params: Optional[List[Any]] = None):
    """
    Execute a write / non-select SQL statement.
    If DB is offline, queue the write to memory + offline file, and attempt reconnection on next change.
    """
    global db_online, db_pending_queue
    params = params or []
    if not db_online or db_client is None:
        # queue the write
        print("[DB] offline, queueing write")
        record = {"sql": sql, "params": params}
        db_pending_queue.append(record)
        # Also persist to offline queue file for crash-safety
        try:
            if os.path.exists(DB_OFFLINE_QUEUE_FILE):
                with open(DB_OFFLINE_QUEUE_FILE, "r", encoding="utf-8") as f:
                    arr = json.load(f)
            else:
                arr = []
            arr.append(record)
            with open(DB_OFFLINE_QUEUE_FILE, "w", encoding="utf-8") as f:
                json.dump(arr, f)
        except Exception as e:
            print(f"[DB] Failed to append to offline queue file: {e}")
        # Try reconnect when a new change is queued (caller should attempt db_reconnect_on_demand)
        raise ConnectionError("DB offline; write queued")
    # If online, run execute
    try:
        # libsql-client execute returns an object with .rows for select; for writes we can just await execute
        if params:
            result = await db_client.execute(sql, params)
        else:
            result = await db_client.execute(sql)
        return result
    except Exception as e:
        # mark offline and queue
        print(f"[DB] Execute failed, marking offline: {e}")
        db_client_local = db_client
        await db_close()
        # queue
        record = {"sql": sql, "params": params}
        db_pending_queue.append(record)
        try:
            if os.path.exists(DB_OFFLINE_QUEUE_FILE):
                with open(DB_OFFLINE_QUEUE_FILE, "r", encoding="utf-8") as f:
                    arr = json.load(f)
            else:
                arr = []
            arr.append(record)
            with open(DB_OFFLINE_QUEUE_FILE, "w", encoding="utf-8") as f:
                json.dump(arr, f)
        except Exception as ex:
            print(f"[DB] Failed to append to offline queue file: {ex}")
        db_online = False
        raise


async def db_query(sql: str, params: Optional[List[Any]] = None):
    """Execute select and return rows (list of tuples)."""
    if not db_online or db_client is None:
        raise ConnectionError("DB offline")
    params = params or []
    try:
        rs = await db_client.execute(sql, params) if params else await db_client.execute(sql)
        # libsql-client returns .rows attribute
        return getattr(rs, "rows", [])
    except Exception as e:
        print(f"[DB] Query failed: {e}")
        # mark offline
        await db_close()
        raise


async def db_reconnect_and_flush_if_needed():
    """Attempt to reconnect if not online, then flush queued writes (from memory + offline file)."""
    global db_pending_queue, db_online
    if db_online and db_client:
        # still online, flush pending if any
        pass
    else:
        # try reconnect
        try:
            print("[DB] Attempting reconnect...")
            await db_connect(db_url, db_token)
        except Exception as e:
            print(f"[DB] Reconnect failed: {e}")
            return False

    # If file queue exists, read and append to pending queue, then flush
    if os.path.exists(DB_OFFLINE_QUEUE_FILE):
        try:
            with open(DB_OFFLINE_QUEUE_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
            # Prepend file entries to pending (so order preserved)
            if arr:
                db_pending_queue = arr + db_pending_queue
        except Exception as e:
            print(f"[DB] Failed to read offline queue file: {e}")

    # Now flush pending queue
    if db_pending_queue:
        try:
            print(f"[DB] Flushing {len(db_pending_queue)} pending writes...")
            for rec in list(db_pending_queue):
                try:
                    sql = rec["sql"]
                    params = rec.get("params", [])
                    await db_execute(sql, params)
                    db_pending_queue.pop(0)
                except Exception as e:
                    print(f"[DB] Failed to flush record, will retry later: {e}")
                    break
            # If queue emptied, remove offline file
            if not db_pending_queue and os.path.exists(DB_OFFLINE_QUEUE_FILE):
                try:
                    os.remove(DB_OFFLINE_QUEUE_FILE)
                except Exception:
                    pass
            return True
        except Exception as e:
            print(f"[DB] Error flushing pending writes: {e}")
            return False
    return True


# ----------------- Logging helper -----------------
async def add_log(project_id: str, user: str, verb: str, name: str, value: str, bot: bool = False):
    """
    Add log entry to memory + DB (or queue if DB offline).
    For --save-file (and not sync), write to file on each change. For sync mode, write only at shutdown.
    """
    entry = {"user": user, "verb": verb, "name": name, "value": value, "timestamp": int(time.time() * 1000)}
    target = bot_logs if bot else logs

    if memory_usage_high():
        # flush memory to DB and clear memory to reduce usage
        try:
            print("[MEMORY] High memory, flushing to DB")
            await flush_memory_to_db()
        except Exception as e:
            print(f"[MEMORY] flush failed: {e}")

    target.setdefault(project_id, []).append(entry)

    # persist to DB if configured
    if db_client:
        try:
            # ensure reconnect+flush if offline
            if not db_online:
                await db_reconnect_and_flush_if_needed()
            await db_execute(
                "INSERT INTO {} (project_id, user, verb, name, value, timestamp) VALUES (?, ?, ?, ?, ?, ?)".format(
                    "bot_logs" if bot else "logs"
                ),
                [project_id, user, verb, name, value, entry["timestamp"]],
            )
        except ConnectionError:
            # write queued already handled inside db_execute
            pass
        except Exception as e:
            print(f"[DB] add_log error: {e}")

    # if save-file is set and not sync mode, save immediately
    if save_file_path and not args.sync:
        save_to_file()

    return entry


# ----------------- Scratch login (username/password) -----------------
async def scratch_login(username: str, password: str):
    """
    Perform POST to Scratch login, store SCRATCH_SESSIONID & SCRATCH_CSRF from returned cookies.
    """
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
            text = await resp.text()
            if resp.status != 200:
                raise Exception(f"Scratch login failed: {resp.status} {text}")
            cookies = session.cookie_jar.filter_cookies("https://scratch.mit.edu")
            sid = cookies.get("scratchsessionsid")
            csrf = cookies.get("scratchcsrftoken")
            if not sid or not csrf:
                raise Exception("Scratch login did not return expected cookies")
            SCRATCH_SESSIONID = sid.value
            SCRATCH_CSRF = csrf.value
            print(f"[SCRATCH] Logged in as {username}")


# ----------------- Scratch validation APIs for clients (uA) -----------------
async def validate_user_exists(user: str) -> bool:
    url = f"https://scratch.mit.edu/users/{user}"
    headers = {"User-Agent": UA_STRING}
    try:
        async with ClientSession(headers=headers) as s:
            async with s.get(url) as resp:
                return resp.status == 200
    except Exception:
        return False


async def validate_client_session(sessionid: str, csrftoken: str) -> bool:
    """
    Validate the client-provided session by attempting to post a comment to project 1219579420.
    """
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
    try:
        async with ClientSession(headers=headers) as s:
            async with s.post(url, json=payload) as resp:
                return resp.status == 200
    except Exception:
        return False


# ----------------- Proxy fetch functions -----------------
async def fetch_from_turbowarp(project_id: str) -> Dict[str, str]:
    uri = "wss://clouddata.turbowarp.org"
    headers = {"User-Agent": UA_STRING}
    out = {}
    try:
        async with ClientSession(headers=headers) as session:
            async with session.ws_connect(uri) as ws:
                await ws.send_json({"method": "handshake", "user": "CLONED_SCRATCH_CLOUD", "project_id": project_id})
                start = time.time()
                while True:
                    timeout = 2 - (time.time() - start)
                    if timeout <= 0:
                        break
                    try:
                        msg = await ws.receive(timeout=timeout)
                    except asyncio.TimeoutError:
                        break
                    if msg.type == WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if data.get("method") == "set":
                            out[data["name"]] = data["value"]
                    elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                        break
    except Exception as e:
        print(f"[PROXY-TURBO] {e}")
        raise
    return out


async def fetch_from_scratch(project_id: str) -> Dict[str, str]:
    uri = "wss://clouddata.scratch.mit.edu"
    headers = {"User-Agent": UA_STRING}
    if SCRATCH_SESSIONID and SCRATCH_CSRF:
        headers["Cookie"] = f"scratchsessionsid={SCRATCH_SESSIONID}; scratchcsrftoken={SCRATCH_CSRF}; scratchlanguage=en"
    out = {}
    try:
        async with ClientSession(headers=headers) as session:
            async with session.ws_connect(uri) as ws:
                await ws.send_json({"method": "handshake", "user": SCRATCH_USER or "CLONED_SCRATCH_CLOUD", "project_id": project_id})
                start = time.time()
                while True:
                    timeout = 2 - (time.time() - start)
                    if timeout <= 0:
                        break
                    try:
                        msg = await ws.receive(timeout=timeout)
                    except asyncio.TimeoutError:
                        break
                    if msg.type == WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if data.get("method") == "set":
                            out[data["name"]] = data["value"]
                    elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                        break
    except Exception as e:
        print(f"[PROXY-SCRATCH] {e}")
        raise
    return out


# ----------------- Memory flush to DB -----------------
async def flush_memory_to_db():
    """
    Write all in-memory projects/logs/bot_logs to DB then clear memory.
    """
    global projects, logs, bot_logs
    if not db_client:
        return
    print("[FLUSH] Flushing memory to DB...")
    try:
        # Begin transaction (libsql-client's execute supports multi-statement or run sequentially)
        # Write projects
        for pid, vars_dict in list(projects.items()):
            for name, value in vars_dict.items():
                try:
                    await db_execute("INSERT OR REPLACE INTO projects (project_id, name, value) VALUES (?, ?, ?)", [pid, name, value])
                except Exception as e:
                    print(f"[FLUSH] project write failed: {e}")
        # Write logs
        for pid, entries in list(logs.items()):
            for e in entries:
                try:
                    await db_execute("INSERT INTO logs (project_id, user, verb, name, value, timestamp) VALUES (?, ?, ?, ?, ?, ?)", [pid, e["user"], e["verb"], e["name"], e["value"], e["timestamp"]])
                except Exception as ee:
                    print(f"[FLUSH] log write failed: {ee}")
        # Write bot_logs
        for pid, entries in list(bot_logs.items()):
            for e in entries:
                try:
                    await db_execute("INSERT INTO bot_logs (project_id, user, verb, name, value, timestamp) VALUES (?, ?, ?, ?, ?, ?)", [pid, e["user"], e["verb"], e["name"], e["value"], e["timestamp"]])
                except Exception as ee:
                    print(f"[FLUSH] bot_log write failed: {ee}")
        # Clear memory
        projects.clear()
        logs.clear()
        bot_logs.clear()
        print("[FLUSH] Completed. Memory cleared.")
    except Exception as e:
        print(f"[FLUSH] Error: {e}")


# ----------------- Scratch Sync Manager (persistent) -----------------
async def scratch_sync_manager():
    """
    Persistent Scratch WS that:
      - Sends queued outgoing payloads (from scratch_sync_queue)
      - Receives Scratch 'set' messages and broadcasts to local clients
    """
    if SCRATCH_SESSIONID is None or SCRATCH_CSRF is None:
        print("[SYNC] Scratch session not set; cannot start sync manager")
        return
    uri = "wss://clouddata.scratch.mit.edu"
    headers = {"User-Agent": UA_STRING, "Cookie": f"scratchsessionsid={SCRATCH_SESSIONID}; scratchcsrftoken={SCRATCH_CSRF}; scratchlanguage=en"}
    backoff = 1.0
    last_local_origin_ids = set()  # to help suppress echoes if needed (we track nothing complex here)

    while True:
        try:
            print("[SYNC] Connecting to Scratch sync...")
            async with ClientSession(headers=headers) as session:
                async with session.ws_connect(uri) as ws:
                    print("[SYNC] Connected")
                    backoff = 1.0
                    # On connect, flush sync queue items to Scratch
                    while not scratch_sync_queue.empty():
                        item = await scratch_sync_queue.get()
                        try:
                            await ws.send_json(item["payload"])
                        except Exception as e:
                            print(f"[SYNC] Failed to send queued payload: {e}")
                            await scratch_sync_queue.put(item)
                            raise

                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue
                            if data.get("method") == "set":
                                pid = str(data.get("project_id", ""))
                                name = data.get("name")
                                value = str(data.get("value"))
                                # Validate value before applying
                                if not validate_value(value):
                                    continue
                                # Update memory/DB
                                if memory_usage_high():
                                    await flush_memory_to_db()
                                projects.setdefault(pid, {})[name] = value
                                await add_log(pid, "ScratchSync", "set_var", name, value, bot=False)
                                # Broadcast to local clients
                                for peer in list(connections.get(pid, set())):
                                    try:
                                        await peer.send_json({"method": "set", "name": name, "value": value, "project_id": pid})
                                    except Exception:
                                        pass
                        elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                            print("[SYNC] Scratch WS closed")
                            break
        except Exception as e:
            print(f"[SYNC] Connection error: {e}")
        # Reconnect backoff
        await asyncio.sleep(min(backoff, 60))
        backoff = min(backoff * 2, 60)


# ----------------- WebSocket handler for clients -----------------
async def ws_handler(request):
    """
    Accepts client WS connections. Expects messages of the form:
    - handshake {project_id, user, ...}
    - set {project_id, name, value, user?}
    """
    global args
    app_args = request.app["args"]
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    ws_id = id(ws)
    ua = request.headers.get("User-Agent", "") or ""
    browser_like = is_browser_like(ua)

    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except Exception:
                continue

            method = data.get("method")
            project_id = str(data.get("project_id", ""))
            user = data.get("user", "UNKNOWN")

            # HANDSHAKE
            if method == "handshake":
                print(f"[WS] Handshake {user} on project {project_id}")
                # If uA is enabled and client appears browser-like, perform user checks
                if app_args.use_authorization and browser_like:
                    # Check user existence
                    ok_user = await validate_user_exists(user)
                    if not ok_user:
                        # Log under bots and reply error + ScratchAuthFailed
                        await ws.send_json({"Error": "Sorry! Banned accounts cannot use the cloud.", "ScratchAuthFailed": "True"})
                        await add_log(project_id, user, "auth_fail", "handshake_user_missing", "banned", bot=True)
                        await ws.close()
                        print(f"[WS] User is banned: {user}")
                        return ws
                    # Expect sessionid + csrftoken from client in handshake
                    sid = data.get("sessionid")
                    csrf = data.get("csrftoken")
                    ok_session = False
                    if sid and csrf:
                        ok_session = await validate_client_session(sid, csrf)
                    if not ok_session:
                        await ws.send_json({"Error": "Sorry! Banned accounts cannot use the cloud.", "ScratchAuthFailed": "True"})
                        await add_log(project_id, user, "auth_fail", "handshake_bad_session", "bad_session", bot=True)
                        await ws.close()
                        print(f"[WS] Could not validate session for user {user}")
                        return ws

                # Register connection to room
                connections.setdefault(project_id, set()).add(ws)
                ws_to_project[ws_id] = project_id

                # Send current vars (from memory)
                for name, value in projects.get(project_id, {}).items():
                    await ws.send_json({"method": "set", "name": name, "value": value, "project_id": project_id})

                # If proxy-scratch enabled and we don't have vars, try proxy fetch (scratch first if --ps)
                if app_args.proxy_scratch and (project_id not in projects or not projects.get(project_id)):
                    vars_from = {}
                    try:
                        vars_from = await fetch_from_scratch(project_id)
                        print(f"[PROXY] Fetched {len(vars_from)} vars from Scratch for {project_id}")
                    except Exception as e:
                        print(f"[PROXY] Scratch failed: {e}, trying TurboWarp")
                        try:
                            vars_from = await fetch_from_turbowarp(project_id)
                            print(f"[PROXY] Fetched {len(vars_from)} vars from TurboWarp for {project_id}")
                        except Exception as e2:
                            print(f"[PROXY] TurboWarp failed: {e2}")
                            vars_from = {}

                    if vars_from:
                        # store and send to client
                        if memory_usage_high():
                            await flush_memory_to_db()
                        projects.setdefault(project_id, {}).update(vars_from)
                        for n, v in vars_from.items():
                            # log as proxy
                            await add_log(project_id, user, "proxy_set_var", n, str(v), bot=not browser_like)
                            await ws.send_json({"method": "set", "name": n, "value": v, "project_id": project_id})

                # If not proxy_scratch and no local vars, try TurboWarp fallback
                elif not app_args.proxy_scratch and (project_id not in projects or not projects.get(project_id)):
                    try:
                        vars_from = await fetch_from_turbowarp(project_id)
                        if vars_from:
                            projects.setdefault(project_id, {}).update(vars_from)
                            for n, v in vars_from.items():
                                await add_log(project_id, user, "proxy_set_var", n, str(v), bot=not browser_like)
                                await ws.send_json({"method": "set", "name": n, "value": v, "project_id": project_id})
                    except Exception:
                        pass

            # SET message
            elif method == "set":
                name = data.get("name")
                value = str(data.get("value"))
                # Validate value: numeric and <1000 chars
                if not validate_value(value):
                    await ws.send_json({"Error": "Invalid value. Must be numeric and <1000 chars."})
                    continue
                print(f"[WS - SET VAR] SET {name} to value {value} by user {user}")
                # store in memory (or flush to DB if memory high)
                if memory_usage_high():
                    await flush_memory_to_db()
                projects.setdefault(project_id, {})[name] = value

                # log to appropriate place (bots vs normal)
                await add_log(project_id, user, "set_var", name, value, bot=not browser_like)

                # broadcast to other local clients in the same room
                for peer in list(connections.get(project_id, set())):
                    if peer is ws:
                        continue
                    try:
                        await peer.send_json({"method": "set", "name": name, "value": value, "project_id": project_id})
                    except Exception:
                        pass

                # If sync enabled, forward to scratch sync manager via queue
                if app_args.sync:
                    payload = {"method": "set", "name": name, "value": value, "project_id": project_id, "user": SCRATCH_USER or ""}
                    await scratch_sync_queue.put({"project_id": project_id, "payload": payload})

            else:
                # ignore unknown methods for now
                pass

    except Exception as e:
        print(f"[WS Handler] Exception: {e}")
    finally:
        # cleanup
        pid = ws_to_project.pop(ws_id, None)
        if pid:
            s = connections.get(pid, set())
            if ws in s:
                s.discard(ws)
            if not s:
                connections.pop(pid, None)
        try:
            await ws.close()
        except Exception:
            pass
    return ws


# ----------------- HTTP endpoints -----------------
async def handle_logs(request):
    pid = request.query.get("project", "")
    return web.json_response(logs.get(pid, []))


async def handle_bots(request):
    pid = request.query.get("project", "")
    return web.json_response(bot_logs.get(pid, []))


# ----------------- CLI & Main -----------------
async def main():
    global args, save_file_path, db_client, db_online, scratch_sync_task

    parser = argparse.ArgumentParser()
    parser.add_argument("--secure-host", "--s", action="store_true", help="Enable TLS (auto-search ssl.crt/ssl.key if not provided)")
    parser.add_argument("--cert", help="TLS cert path (optional)")
    parser.add_argument("--key", help="TLS key path (optional)")

    parser.add_argument("--proxy-scratch", "--ps", action="store_true", help="Proxy to Scratch first, fallback to TurboWarp (requires --session)")
    parser.add_argument("--sync", action="store_true", help="Persistent sync with Scratch (requires --session)")
    parser.add_argument("--save-db", help="DB URL for libsql-client (file:cloud.db, sql://cloud.db, libsql://..., https://..., wss://...)")
    parser.add_argument("--db-token", help="Optional DB auth token for remote libSQL")
    parser.add_argument("--save-file", help="JSON save file path")
    parser.add_argument("--session", nargs=2, metavar=("USER", "PASS"), help="Scratch username and password (required for --ps or --sync)")
    parser.add_argument("--use-authorization", "--uA", action="store_true", help="Authenticate browser-like clients on every handshake")
    parser.add_argument("--ua", help="Override UA string", default=None)

    args = parser.parse_args()

    # UA override
    global UA_STRING
    if args.ua:
        UA_STRING = args.ua
    print(f"[CONFIG] UA: {UA_STRING}")

    # save-file
    save_file_path = args.save_file

    # DB init (libsql-client)
    if args.save_db:
        # Normalize save_db url:
        db_input = args.save_db
        # if input looks like plain file path (no scheme), convert to file:
        if "://" not in db_input and not db_input.startswith("file:"):
            db_input = f"file:{db_input}"
        # If sql:// scheme present, convert to file:
        if db_input.startswith("sql://"):
            db_input = "file:" + db_input[len("sql://"):]
        # If libsql:// treat as wss: or keep as libsql per libsql client docs (libsql: == wss:)
        # For python libsql-client create_client accepts a dict with url & authToken
        await db_connect(db_input, args.db_token)

        # If offline queue exists, attempt to flush immediately
        await db_reconnect_and_flush_if_needed()

    # load save-file (if any)
    if save_file_path:
        load_from_file()

    # scratch session login if provided
    if (args.proxy_scratch or args.sync or args.use_authorization) and not args.session:
        # these modes require session per previous design
        raise Exception("--proxy-scratch, --sync or --uA requires --session USER PASS")
    if args.session:
        username, password = args.session
        await scratch_login(username, password)

    # start scratch sync manager if requested
    if args.sync:
        scratch_sync_task = asyncio.create_task(scratch_sync_manager())

    # Build app
    app = web.Application()
    app["args"] = args
    app.router.add_get("/", handle_root)
    app.router.add_get("/logs", handle_logs)
    app.router.add_get("/bots", handle_bots)
    app.router.add_routes([web.route("*", "/{tail:.*}", handle_404)])

    # Setup TLS if requested
    ssl_ctx = None
    if args.secure_host:
        cert_path = args.cert or "ssl.crt"
        key_path = args.key or "ssl.key"
        if not (os.path.exists(cert_path) and os.path.exists(key_path)):
            raise Exception(f"TLS enabled but cert/key not found at {cert_path} / {key_path}")
        ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain(cert_path, key_path)
        print(f"[TLS] Using {cert_path} and {key_path}")

    # Start server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8765, ssl_context=ssl_ctx)
    await site.start()
    proto = "wss" if args.secure_host else "ws"
    print(f"[SERVER] Listening on {proto}://0.0.0.0:8765/")

    # Graceful shutdown handling
    try:
        await asyncio.Future()  # run forever
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        print("[SHUTDOWN] KeyboardInterrupt received")
    finally:
        print("[SHUTDOWN] Cleaning up...")
        # save-file on shutdown if requested
        if save_file_path:
            save_to_file()

        # flush memory to DB if configured
        if db_client:
            try:
                await flush_memory_to_db()
            except Exception as e:
                print(f"[SHUTDOWN] flush failed: {e}")
            try:
                await db_close()
            except Exception:
                pass

        if scratch_sync_task:
            scratch_sync_task.cancel()
            try:
                await scratch_sync_task
            except Exception:
                pass

        await runner.cleanup()
        print("[SHUTDOWN] Done.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted by user")
