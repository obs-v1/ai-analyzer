#!/usr/bin/env python3
"""Receive Alertmanager webhooks and print a Cursor diagnosis.

Each POST is acknowledged immediately, then a local Cursor agent inspects the
bankobs namespace with read-only kubectl and the diagnosis is printed.

    pip install -r requirements.txt
    export CURSOR_API_KEY='cursor_...'
    python3.11 webhook.py
    python3.11 webhook.py --api-key "$CURSOR_API_KEY" --model composer-2.5
"""

import argparse
import json
import os
import queue
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from cursor_sdk import (
    Agent,
    AgentOptions,
    Cursor,
    CursorAgentError,
    CustomTool,
    CustomToolContext,
    LocalAgentOptions,
    ModelSelection,
)

KUBE_CONTEXT = "obs-v1-bankobs"
KUBE_NAMESPACE = "bankobs"
ALLOWED_VERBS = {"get", "describe", "logs", "top", "api-resources", "explain"}
CLUSTER_VERBS = {"api-resources", "explain"}
KUBECTL_TIMEOUT_SECONDS = 45
OUTPUT_LIMIT = 20000

jobs = queue.Queue()
print_lock = threading.Lock()
settings = {}


def emit(text):
    with print_lock:
        print(text, flush=True)


def run_kubectl(args, _context: CustomToolContext):
    """Run a read-only kubectl command in the bankobs namespace."""
    argv = args.get("args")
    if not isinstance(argv, list) or not argv or not all(isinstance(part, str) for part in argv):
        return 'args must be a list of strings, for example ["get", "pods"]'

    verb = argv[0]
    if verb not in ALLOWED_VERBS:
        allowed = ", ".join(sorted(ALLOWED_VERBS))
        return f"rejected: {verb!r} is not allowed. Use one of: {allowed}"

    cleaned = []
    skip_next = False
    for part in argv:
        if skip_next:
            skip_next = False
            continue
        if part in ("--context", "-n", "--namespace"):
            skip_next = True
            continue
        if part.startswith(("--context=", "--namespace=", "-n=")):
            continue
        cleaned.append(part)

    command = ["kubectl", "--context", KUBE_CONTEXT]
    if verb not in CLUSTER_VERBS:
        command.extend(["-n", KUBE_NAMESPACE])
    command.extend(cleaned)

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=KUBECTL_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return f"kubectl timed out after {KUBECTL_TIMEOUT_SECONDS}s: {' '.join(command)}"
    except OSError as err:
        return f"kubectl could not start: {err}"

    output = (completed.stdout or "") + (completed.stderr or "")
    if len(output) > OUTPUT_LIMIT:
        output = output[:OUTPUT_LIMIT] + "\n...[truncated]"
    header = f"$ {' '.join(command)}\nexit {completed.returncode}\n"
    return header + (output or "(no output)")


KUBECTL_TOOL = CustomTool(
    description=(
        "Run a read-only kubectl command. Context is always obs-v1-bankobs and "
        "the namespace is always bankobs. args is the kubectl argument list "
        'without context or namespace, for example ["get", "pods"] or '
        '["describe", "pod", "payments-0"] or ["logs", "payments-0", "--tail=100"].'
    ),
    input_schema={
        "type": "object",
        "properties": {
            "args": {
                "type": "array",
                "items": {"type": "string"},
                "description": "kubectl arguments, starting with the verb",
            }
        },
        "required": ["args"],
    },
    execute=run_kubectl,
)


def alert_names(payload):
    names = []
    for alert in payload.get("alerts") or []:
        labels = alert.get("labels") or {}
        name = labels.get("alertname") or "unknown"
        status = alert.get("status") or payload.get("status") or ""
        names.append(f"{name} ({status})" if status else name)
    return names or ["(no alerts)"]


def analyze(payload):
    names = ", ".join(alert_names(payload))
    body = json.dumps(payload, indent=2, sort_keys=True)
    emit(
        f"\n{'=' * 60}\n"
        f"ALERT PAYLOAD  {names}\n"
        f"receiver={payload.get('receiver')} status={payload.get('status')}\n"
        f"{'-' * 60}\n"
        f"{body}\n"
        f"{'-' * 60}\n"
        "Analysis running"
    )
    prompt = (
        "A Prometheus Alertmanager webhook just arrived. Diagnose what is wrong "
        f"in Kubernetes context {KUBE_CONTEXT}, namespace {KUBE_NAMESPACE}.\n"
        "You must run kubectl before you answer. Prefer the kubectl tool. "
        "If you use the shell instead, the command must start with "
        f"kubectl --context {KUBE_CONTEXT} -n {KUBE_NAMESPACE}. "
        "Only read the cluster: get, describe, logs, top. "
        "Do not create, delete, or change anything. "
        "Base the conclusion on command output.\n"
        "If this alert is not about a workload in that namespace, say so after "
        "checking, and report what the namespace actually shows.\n"
        "Reply with what is wrong: the alert, the observed cluster state, and "
        "the most likely cause. Keep it short.\n\n"
        "Webhook payload:\n"
        f"{json.dumps(payload, indent=2, sort_keys=True)}"
    )
    try:
        result = Agent.prompt(
            prompt,
            AgentOptions(
                api_key=settings["api_key"],
                model=settings["model"],
                local=LocalAgentOptions(
                    cwd=os.getcwd(),
                    custom_tools={"kubectl": KUBECTL_TOOL},
                ),
            ),
        )
    except CursorAgentError as err:
        emit(f"Cursor could not start: {err.message} (retryable={err.is_retryable})")
        return

    if result.status != "finished":
        detail = result.error_message or result.result or ""
        code = f" {result.error_code}" if result.error_code else ""
        emit(f"Analysis failed ({result.status}{code}).\n{detail}".rstrip())
        return
    report = result.result or "(Cursor returned an empty analysis)"
    emit(f"ANALYSIS REPORT  {names}\n{'-' * 60}\n{report}\n{'=' * 60}")


def worker():
    while True:
        payload = jobs.get()
        try:
            analyze(payload)
        except Exception as err:
            emit(f"Analysis error: {err}")
        finally:
            jobs.task_done()


class WebhookHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self._send(200, {"status": "ok"})

    def do_POST(self):
        self._accept()

    def do_PUT(self):
        self._accept()

    def _accept(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(raw.decode("utf-8") or "null")
        except json.JSONDecodeError:
            emit("Ignored webhook: body is not JSON")
            self._send(400, {"status": "invalid json"})
            return
        if not isinstance(payload, dict):
            emit("Ignored webhook: JSON body is not an object")
            self._send(400, {"status": "invalid payload"})
            return
        jobs.put(payload)
        emit(f"Received {', '.join(alert_names(payload))} — queued")
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


def resolve_model(model_id, api_key):
    """Use the account's default variant. Plus rejects a bare model id."""
    try:
        catalog = Cursor.models.list(api_key=api_key)
    except CursorAgentError as err:
        emit(f"Could not list models ({err.message}); using {model_id} as given")
        return model_id
    for model in catalog:
        if model.id != model_id:
            continue
        for variant in model.variants:
            if variant.is_default and variant.params:
                return ModelSelection(id=model.id, params=list(variant.params))
        return model.id
    known = ", ".join(model.id for model in catalog) or "(none)"
    emit(f"Model {model_id} is not in this account's catalog ({known})")
    return model_id


def main():
    parser = argparse.ArgumentParser(description="Diagnose Alertmanager webhooks with Cursor")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--api-key", default=os.environ.get("CURSOR_API_KEY", ""))
    parser.add_argument("--model", default=os.environ.get("CURSOR_MODEL", "composer-2.5"))
    args = parser.parse_args()

    if not args.api_key.strip():
        raise SystemExit("Set CURSOR_API_KEY or pass --api-key")

    settings["api_key"] = args.api_key.strip()
    settings["model"] = resolve_model(args.model, settings["api_key"])
    threading.Thread(target=worker, daemon=True).start()

    server = ThreadingHTTPServer((args.host, args.port), WebhookHandler)
    chosen = settings["model"]
    if isinstance(chosen, ModelSelection):
        params = ", ".join(f"{param.id}={param.value}" for param in chosen.params)
        model_label = f"{chosen.id} ({params})" if params else chosen.id
    else:
        model_label = str(chosen)
    emit(
        f"Listening on http://{args.host}:{args.port}/ "
        f"(kubectl --context {KUBE_CONTEXT} -n {KUBE_NAMESPACE}, model {model_label})"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        emit("\nStopping")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
