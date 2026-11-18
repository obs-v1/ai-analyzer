#!/usr/bin/env python3
"""Capture Prometheus Alertmanager webhook payloads and print them.

Alertmanager posts JSON to the receiver URL. This server accepts that body
on POST / (any path is accepted) and writes it to stdout.

Run:
    python webhook.py
    python webhook.py --host 0.0.0.0 --port 8080

Point Alertmanager at it:

    receivers:
      - name: ai-analyzer
        webhook_configs:
          - url: http://<this-host>:8080/
            send_resolved: true
"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class WebhookHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self._send(200, {"status": "ok"})

    def do_POST(self):
        self._capture()

    def do_PUT(self):
        self._capture()

    def _capture(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length else b""
        text = raw.decode("utf-8", errors="replace")

        print("=" * 60, flush=True)
        print(f"{self.command} {self.path}", flush=True)
        for key, value in self.headers.items():
            print(f"{key}: {value}", flush=True)
        print("-" * 60, flush=True)
        if not text:
            print("(empty body)", flush=True)
        else:
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                print(text, flush=True)
            else:
                print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        print("=" * 60, flush=True)

        self._send(200, {"status": "received"})

    def _send(self, code, body):
        encoded = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, fmt, *args):
        return


def main():
    parser = argparse.ArgumentParser(description="Print Alertmanager webhook payloads")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), WebhookHandler)
    print(f"Listening on http://{args.host}:{args.port}/", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping", flush=True)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
