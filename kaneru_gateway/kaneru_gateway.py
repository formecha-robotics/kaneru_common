from flask import Flask, request, Response
from flask_cors import CORS
import requests
import time
import datetime
import json
import os
import uuid
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from typing import Optional, Dict, Any
from production.internal_jwt import mint_internal_jwt

# -------------------------
# Prometheus metrics
# -------------------------

REQ_LATENCY = Histogram(
    "kaneru_request_latency_seconds",
    "Request latency",
    ["method", "endpoint"]
)

REQ_COUNT = Counter(
    "kaneru_request_total",
    "Total requests",
    ["method", "endpoint", "http_status"]
)

SUCCESS = 200

# -------------------------
# App setup
# -------------------------

app = Flask(__name__)
CORS(app)

# -------------------------
# Config helpers
# -------------------------

def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def env_int(name: str, default: int) -> int:
    try:
        return int(env(name, str(default)))
    except ValueError:
        return default


def jst_timestamp():
    """Return timestamp in Gunicorn-ish format with JST timezone."""
    jst = datetime.timezone(datetime.timedelta(hours=9))
    dt = datetime.datetime.now(tz=jst)
    return dt.strftime("[%Y-%m-%d %H:%M:%S +0900]")


def gw_log(message: str):
    """Unified gateway logger with timestamp + PID."""
    pid = os.getpid()
    print(f"{jst_timestamp()} [{pid}] [KANERU-GW] {message}", flush=True)


def base_url_for(service_key: str) -> str:
    """
    Compute a service base URL from env vars.

    Preferred per-service env:
      KANERU_<KEY>_URL  (e.g., KANERU_AUTH_URL=http://auth:8001)

    Or host/port combo:
      KANERU_<KEY>_HOST (e.g., auth)
      KANERU_<KEY>_PORT (e.g., 8001)

    Fallback defaults (good for local dev):
      127.0.0.1:<default_port>
    """
    key = service_key.upper()

    url = os.getenv(f"KANERU_{key}_URL")
    if url:
        return url.rstrip("/")

    host_default = "127.0.0.1"
    port_defaults = {
        "AUTH": 8001,
        "SCANNER": 8002,
        "USER_DETAILS": 8003,
        "KANERU_JOB": 8004,
        "MAPS": 8005,
    }

    host = env(f"KANERU_{key}_HOST", host_default)
    port = env_int(f"KANERU_{key}_PORT", port_defaults.get(key, 8000))
    scheme = env("KANERU_SCHEME", "http")
    return f"{scheme}://{host}:{port}"


# -------------------------
# Routing table (env-driven)
# -------------------------

ROUTING_TABLE = {
    "/auth": base_url_for("AUTH"),
    "/scanner": base_url_for("SCANNER"),
    "/user_details": base_url_for("USER_DETAILS"),
    "/kaneru_job": base_url_for("KANERU_JOB"),
    "/maps": base_url_for("MAPS"),
}

DEFAULT_BACKEND = base_url_for(env("KANERU_DEFAULT_SERVICE", "SCANNER"))


# -------------------------
# Core routing logic
# -------------------------

def find_backend(path: str) -> str:
    """
    Longest prefix wins (important for nested routes).
    """
    matches = [(prefix, target) for prefix, target in ROUTING_TABLE.items() if path.startswith(prefix)]
    if not matches:
        return DEFAULT_BACKEND
    prefix, target = max(matches, key=lambda x: len(x[0]))
    return target


def forward_request(target_url, service, scope):
    timeout_s = env_int("KANERU_GATEWAY_TIMEOUT_SECONDS", 10)

    hop_by_hop = {
        "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailers", "transfer-encoding", "upgrade"
    }

    try:
        data = request.get_data()
        headers = {k: v for k, v in request.headers if k.lower() != "host"}
        path = request.path or ""
        rid = request.headers.get("X-Request-Id") or str(uuid.uuid4())

        token = mint_internal_jwt(
            audience=(service if service !="" else "scanner"),
            scopes=[scope],
            rid=rid,
            ttl_seconds=30,
        )
                    
        if service != "auth":
            session_key = request.headers.get("Authorization")
            x_user_id = request.headers.get("X-User-Id")
            print(session_key)
            print(x_user_id)
            
            payload = {"session_key": session_key, "x_user_id" : x_user_id}
            auth_data = json.dumps(payload).encode("utf-8")           
            
            resp = requests.request(
                method = "POST",
                url = ROUTING_TABLE['/auth']+"/auth/validate_api_permission",
                headers = {
                    "Authorization": "Bearer {}".format(token),
                    "X-Request-Id" : rid,
                    "Content-Type": "application/json",
                },
                data=auth_data,
                timeout=5,
            )
            
            if resp.status_code != SUCCESS:
            
                out_headers = {k: v for k, v in resp.headers.items() if k.lower() not in hop_by_hop}
                return Response(resp.content, status=resp.status_code, headers=out_headers)

        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=data,
            params=request.args,
            timeout=timeout_s,
        )

        out_headers = {k: v for k, v in resp.headers.items() if k.lower() not in hop_by_hop}

        return Response(resp.content, status=resp.status_code, headers=out_headers)

        
    except requests.exceptions.RequestException as e:
        return Response("Upstream error: {}".format(e), status=502)




# -------------------------
# Main route handler
# -------------------------

@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
def gateway(path: str):


    start = time.time()
    
    service, route = (path.strip("/").split("/", 1) + [""])[:2]
    
    escaped_path = "/" + service
    scope = path.strip("/").replace("/", ".")

    user_id = request.headers.get("X-User-Id") or "anonymous"
    backend = find_backend(escaped_path)
    
    target_url = backend + escaped_path + (("/" + route) if route != "" else "")
    
    response = forward_request(target_url, service, scope)

    latency_seconds = time.time() - start
    latency_ms = latency_seconds * 1000.0

    gw_log(
        f"{request.method} {escaped_path} "
        f"-> {backend} "
        f"user={user_id} "
        f"status={response.status_code} "
        f"latency={latency_ms:.2f}ms "
        f"ip={request.remote_addr}"
    )

    # Metrics
    try:
        REQ_LATENCY.labels(request.method, escaped_path).observe(latency_seconds)
        REQ_COUNT.labels(request.method, escaped_path, str(response.status_code)).inc()
    except Exception as e:
        # Never break routing due to metrics issues
        gw_log(f"METRICS_ERROR {escaped_path}: {e}")

    return response


# -------------------------
# Metrics endpoint
# -------------------------

@app.route("/metrics", methods=["GET"])
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


# -------------------------
# Optional: print effective routing table at startup
# -------------------------

if __name__ == "__main__":
    gw_log("Starting Kaneru Gateway")
    for k, v in ROUTING_TABLE.items():
        gw_log(f"ROUTE {k} -> {v}")
    gw_log(f"DEFAULT_BACKEND -> {DEFAULT_BACKEND}")
    port = env_int("KANERU_GATEWAY_PORT", 5050)
    app.run(host="0.0.0.0", port=port)

