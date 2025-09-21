import asyncio
import json
import time
from aiohttp import web, WSMsgType, ClientSession

projects = {}
logs = {}

def add_log(project_id, user, verb, name, value):
    entry = {
        "user": user,
        "verb": verb,
        "name": name,
        "value": value,
        "timestamp": int(time.time() * 1000)
    }
    logs.setdefault(project_id, []).append(entry)
    print(f"Added log for project {project_id}: {entry}")  # << ADD THIS
    return entry

# ---------------- Proxy to TurboWarp ----------------
async def fetch_from_turbowarp(project_id):
    uri = "wss://clouddata.turbowarp.org"
    results = {}

    async with ClientSession() as session:
        async with session.ws_connect(uri) as ws:
            # Send handshake message
            await ws.send_json({
                "method": "handshake",
                "user": "CLONED_SCRATCH_CLOUD",
                "project_id": project_id
            })

            start_time = time.time()
            while True:
                # Timeout after 2 seconds total or no messages
                timeout = 2 - (time.time() - start_time)
                if timeout <= 0:
                    break

                try:
                    msg = await ws.receive(timeout=timeout)
                except asyncio.TimeoutError:
                    print("Timeout waiting for TurboWarp messages")
                    break

                if msg.type == WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    print(f"Received from TurboWarp: {data}")  # Debug print
                    if data.get("method") == "set":
                        results[data["name"]] = data["value"]
                elif msg.type == WSMsgType.PING:
                    print("Received ping, sending pong")
                    await ws.pong()
                elif msg.type == WSMsgType.PONG:
                    print("Received pong")
                elif msg.type == WSMsgType.CLOSE:
                    print("TurboWarp closed connection")
                    break
                elif msg.type == WSMsgType.ERROR:
                    print(f"WebSocket error: {ws.exception()}")
                    break

    print(f"TurboWarp fetch complete: {len(results)} variables")
    return results
# ---------------- WebSocket Handler (at /) ----------------
async def ws_handler(request):
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return web.Response(text="This endpoint is for WebSocket connections only", status=400)

    ws = web.WebSocketResponse()
    await ws.prepare(request)


    try:
      async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            data = json.loads(msg.data)
            method = data.get("method")
            project_id = str(data.get("project_id"))
            user = data.get("user", "UNKNOWN")

            if method == "handshake":
                if project_id == "1":
                    await ws.send_json({
                        "method": "set",
                        "name": "☁ Anything",
                        "value": "12345",
                        "user": user,
                        "project_id": project_id
                    })

                elif project_id not in projects:
                    print(f"[HANDSHAKE] invalid project {project_id}, proxying to TurboWarp")
                    vars_from_tw = await fetch_from_turbowarp(project_id)
                    projects[project_id] = vars_from_tw

                    # Add logs for each variable received from TurboWarp
                    for name, value in vars_from_tw.items():
                        print(f"Logging variable from TurboWarp: {name} = {value}")
                        add_log(project_id, "TurboWarpProxy", "set_var", name, value)

                    # Send to client
                    for name, value in vars_from_tw.items():
                        await ws.send_json({
                            "method": "set",
                            "name": name,
                            "value": value,
                            "user": user,
                            "project_id": project_id
                        })

                else:
                    for name, value in projects[project_id].items():
                        await ws.send_json({
                            "method": "set",
                            "name": name,
                            "value": value,
                            "user": user,
                            "project_id": project_id
                        })
            elif method == "set":
                name = data.get("name")
                value = data.get("value")
                projects.setdefault(project_id, {})
                projects[project_id][name] = value
                add_log(project_id, user, "set_var", name, value)

                await ws.send_json({
                    "method": "set",
                    "name": name,
                    "value": value,
                    "user": user,
                    "project_id": project_id
                })
    except Exception as e:
       print(f"WebSocket error: {e}")
    finally:
       print("Client disconnected")
       await ws.close()


    return ws

# ---------------- HTTP Logs ----------------
async def handle_logs(request):
    project_id = request.query.get("project")
    limit = int(request.query.get("limit", 50))
    offset = int(request.query.get("offset", 0))
    if not project_id:
        return web.json_response({"error": "Missing project parameter"}, status=400)

    project_logs = logs.get(project_id, [])
    sliced = project_logs[offset:offset+limit]
    return web.json_response(sliced)

# ---------------- Main ----------------
async def main():
    app = web.Application()
    app.router.add_get("/", ws_handler)  # ✅ WebSocket at "/"
    app.router.add_get("/logs", handle_logs)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 8765)
    await site.start()

    print("Mock cloud server running on ws://localhost:8765/")
    print("HTTP logs at http://localhost:8765/logs?project=<id>&limit=10&offset=0")

    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
