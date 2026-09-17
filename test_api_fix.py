"""
Verification harness for the 'Invalid Version' connect bug.

Runs a local mock of the Rubika Bot API:
  - /v1/{token}/{method}: legacy behavior — REJECTS bodies without "version":
        {"status": "INVALID_INPUT", "dev_message": "Invalid Version"}
    Replies wrap payload under "result".
  - /v3/{token}/{method}: current official behavior — no version field needed,
    replies wrap payload under "data".

Then drives the REAL code from main.py (rubika() + extractors) against it.

Run:
  python test_api_fix.py v1   # user's old config (RUBIKA_API_BASE -> /v1)
  python test_api_fix.py v3   # new default (RUBIKA_API_BASE -> /v3)
"""
import asyncio
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 8771

# ---------------------------------------------------------------------------
# Mock Rubika server
# ---------------------------------------------------------------------------
REQUESTS = []  # (path, body) for assertions


class MockRubika(BaseHTTPRequestHandler):
    def log_message(self, *a):  # silence
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except Exception:
            body = {}
        REQUESTS.append((self.path, body))
        parts = self.path.strip("/").split("/")
        version, token, method = (parts + [None, None, None])[:3]
        if token == "BADTOKEN":
            self._reply({"status": "INVALID_TOKEN", "message": "Token is invalid"})
            return
        if version == "v1" and body.get("version") != "1.0":
            self._reply({"status": "INVALID_INPUT", "dev_message": "Invalid Version"})
            return

        if method == "getMe":
            bot = {"id": "1", "first_name": "TestBot", "username": "test_bot"}
            if version == "v1":
                self._reply({"status": "OK", "result": {"bot": bot}})
            else:
                # official v3 shape (rubika_bot lib: Bot(**body["data"]))
                self._reply({"status": "OK", "data": bot})
        elif method == "getUpdates":
            updates = [
                {
                    "type": "NewMessage",
                    "chat_id": "b0abc123",
                    "new_message": {
                        "message_id": 12345,
                        "text": "/start",
                        "time": "1700000000",
                        "is_edited": False,
                        "sender_type": "User",
                        "sender_id": "u0def456",
                        "aux_data": None,
                    },
                },
                {
                    "type": "InlineMessage",
                    "inline_message": {
                        "chat_id": "b0abc123",
                        "message_id": 12346,
                        "text": "\u06f0\u06f1 \u0634\u0631\u0648\u0639",
                        "aux_data": {"button_id": "btn_0_0_abcd1234"},
                    },
                },
            ]
            payload = {"updates": updates, "next_offset_id": "123"}
            key = "result" if version == "v1" else "data"
            self._reply({"status": "OK", key: payload})
        elif method == "sendMessage":
            key = "result" if version == "v1" else "data"
            self._reply({"status": "OK", key: {"message_id": "999"}})
        else:
            self._reply({"status": "UNKNOWN_METHOD", "message": f"no {method}"})

    def _reply(self, obj):
        data = json.dumps(obj).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def main(mode: str):
    base = f"http://127.0.0.1:{PORT}/{mode}"
    os.environ["RUBIKA_API_BASE"] = base
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import main  # fresh import per process (env read at import time)

    failures = []

    def check(name, cond, extra=""):
        status = "PASS" if cond else "FAIL"
        print(f"  [{status}] {name}{(' — ' + str(extra)) if extra and not cond else ''}")
        if not cond:
            failures.append(name)

    async def run():
        # 1) getMe — the exact call that failed in the UI
        data = await main.rubika("getMe", "GOODTOKEN")
        check("getMe succeeded", isinstance(data, dict) and data.get("status") == "OK", data)

        bot = main.extract_bot_info(data)
        check(
            "bot info extracted (first_name)",
            bot.get("first_name") == "TestBot" and bot.get("username") == "test_bot",
            bot,
        )

        # 2) getUpdates with v3/v1 envelope
        data2 = await main.rubika("getUpdates", "GOODTOKEN", {"limit": 100, "offset_id": "0"})
        updates = main.extract_updates(data2)
        check("2 updates extracted", len(updates) == 2, updates)
        u0 = main.extract_update_kind(updates[0])
        check(
            "message update parsed",
            u0["kind"] == "message"
            and u0["chat_id"] == "b0abc123"
            and u0["text"] == "/start"
            and u0["message_id"] == "12345",
            u0,
        )
        u1 = main.extract_update_kind(updates[1])
        check(
            "callback update parsed",
            u1["kind"] == "callback"
            and u1["chat_id"] == "b0abc123"
            and u1["button_id"] == "btn_0_0_abcd1234",
            u1,
        )
        check("next_offset_id extracted", main.extract_next_offset(data2) == "123", data2)

        # 3) sendMessage (keypad payload as built by the app)
        kp = main.build_keypad([["\U0001F680 \u0634\u0631\u0648\u0639", "\u2139\uFE0F \u0631\u0627\u0647\u0646\u0645\u0627"]])
        res = await main.rubika(
            "sendMessage",
            "GOODTOKEN",
            {"chat_id": "b0abc123", "text": "hello", "chat_keypad": kp, "chat_keypad_type": "New"},
        )
        check("sendMessage succeeded", res.get("status") == "OK", res)

        # 4) bad token -> readable error, NO retry storm
        try:
            await main.rubika("getMe", "BADTOKEN")
            check("bad token raises", False)
        except RuntimeError as exc:
            check("bad token error is readable", "Token is invalid" in str(exc), exc)

        # 5) variant memory / request count assertions
        if mode == "v1":
            check(
                "learned working variant (v1 + version field)",
                (main._working_base or "").endswith("/v1") and main._send_version_field is True,
                (main._working_base, main._send_version_field),
            )
            v1_hits = [b for p, b in REQUESTS if p.startswith("/v1/") and "GOODTOKEN" in p]
            without_version = [b for b in v1_hits if "version" not in b]
            check(
                "only ONE probe without version (the first attempt)",
                len(without_version) == 1,
                v1_hits,
            )
        else:
            check(
                "v3 works on first try (no fallback needed)",
                main._working_base is None,
                (main._working_base, main._send_version_field),
            )

    asyncio.run(run())

    if failures:
        print(f"\nRESULT: {len(failures)} FAILURE(S) in mode={mode}")
        sys.exit(1)
    print(f"\nRESULT: ALL PASS in mode={mode}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "v1"
    server = ThreadingHTTPServer(("127.0.0.1", PORT), MockRubika)
    import threading

    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        main(mode)
    finally:
        server.shutdown()
