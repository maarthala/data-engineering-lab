"""connect-secrets — k8s: the kafka-connect pod's helper container. cdc-runner hands it the files a
connector's ${dir:…} references (passwords, keys); it writes them into the folder it shares with
Kafka Connect (an emptyDir: never on disk, gone with the pod). Locally the two share a volume and
this isn't needed. Only cdc-runner may reach it (NetworkPolicy), and every call carries a token.
  PUT    /<dir>/<name>   body = the value          GET /   → the dirs there (JSON)
  DELETE /<dir>          a deleted job's files
"""
import http.server
import json
import os
import re
import shutil

ROOT = os.environ.get("CDC_SECRETS", "/run/lab-secrets")
TOKEN = os.environ.get("CDC_SECRETS_TOKEN", "")
PORT = int(os.environ.get("PORT", "8090"))
SAFE = re.compile(r"^cdc-\d+$")
KEY = re.compile(r"^[a-z0-9-]{1,40}$")


class H(http.server.BaseHTTPRequestHandler):
    def _ok(self, code=204, body=None):
        raw = json.dumps(body).encode() if body is not None else b""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _parts(self):
        if not TOKEN or self.headers.get("X-Token") != TOKEN:
            self._ok(403, {"error": "token"})
            return None
        return [p for p in self.path.split("/") if p]

    def do_GET(self):
        if self._parts() is None:
            return
        self._ok(200, sorted(d for d in os.listdir(ROOT) if SAFE.fullmatch(d)) if os.path.isdir(ROOT) else [])

    def do_PUT(self):
        p = self._parts()
        if p is None:
            return
        if len(p) != 2 or not SAFE.fullmatch(p[0]) or not KEY.fullmatch(p[1]):
            return self._ok(400, {"error": "path"})
        value = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        d = os.path.join(ROOT, p[0])
        os.makedirs(d, exist_ok=True)
        tmp = os.path.join(d, "." + p[1])
        with open(tmp, "wb") as f:
            f.write(value)
        os.chmod(tmp, 0o644)
        os.replace(tmp, os.path.join(d, p[1]))
        self._ok()

    def do_DELETE(self):
        p = self._parts()
        if p is None:
            return
        if len(p) != 1 or not SAFE.fullmatch(p[0]):
            return self._ok(400, {"error": "path"})
        shutil.rmtree(os.path.join(ROOT, p[0]), ignore_errors=True)
        self._ok()

    def log_message(self, *a):                          # no request log (paths name jobs, never values)
        pass


if __name__ == "__main__":
    os.makedirs(ROOT, exist_ok=True)
    http.server.ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
