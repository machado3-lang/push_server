"""
Servidor Push — Control iD iDFace  (v2.0 – Modo Nuvem)
========================================================

O equipamento faz GET/POST /push periodicamente:
  • GET  /push  → busca comandos pendentes
  • POST /push  → envia eventos de acesso + recebe comandos

Endpoints do protocolo Push (para o iDFace):
  GET/POST /push     — poll do equipamento (pode incluir eventos no body)
  POST     /result   — resultado de comando executado
  POST     /event    — endpoint alternativo para eventos de acesso

Endpoints internos (para o controlid.py):
  GET  /health       — status geral do servidor
  GET  /devices      — lista dispositivos (com status online/offline)
  GET  /events       — eventos de acesso recebidos
  GET  /results      — resultados de comandos
  POST /cmd          — enfileira comando para o equipamento
  POST /clear        — limpa fila de resultados e comandos
  POST /clear_events — limpa histórico de eventos
"""

from flask import Flask, request, jsonify
from collections import deque
from datetime import datetime, timedelta
import threading
import uuid
import os

app = Flask(__name__)

# ── Estado global (thread-safe) ──────────────────────────────────────────────
lock         = threading.Lock()
cmd_queue    = deque()               # comandos aguardando o equipamento
results      = deque(maxlen=500)     # resultados de comandos executados
events       = deque(maxlen=2000)    # eventos de acesso vindos do equipamento
devices      = {}                    # {device_key: {last_seen, ip, serial, ...}}
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", "minha_chave_secreta")

# Tempo máximo sem poll para considerar dispositivo offline (segundos)
OFFLINE_TIMEOUT = int(os.environ.get("OFFLINE_TIMEOUT", 90))


# ── Helpers ───────────────────────────────────────────────────────────────────

def now_str():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def check_token(req):
    token = req.headers.get("X-Access-Token") or req.args.get("token")
    return token == ACCESS_TOKEN


def _resolve_device_id(req):
    """
    Monta identificador único e estável para o dispositivo.
    Prioridade:
      1. Header X-Device-Serial
      2. Query param serial
      3. Header X-Device-Id
      4. Query param device_id
      5. IP remoto (fallback)
    """
    serial = (req.headers.get("X-Device-Serial")
              or req.args.get("serial", "").strip())
    ip = req.remote_addr

    if serial:
        return f"{serial}@{ip}", serial, ip

    dev_id = (req.headers.get("X-Device-Id")
              or req.args.get("device_id", "").strip()
              or ip)
    return dev_id, None, ip


def _is_online(info):
    """True se o dispositivo fez poll nos últimos OFFLINE_TIMEOUT segundos."""
    try:
        last = datetime.strptime(info.get("last_seen", ""), "%d/%m/%Y %H:%M:%S")
        return (datetime.now() - last).total_seconds() < OFFLINE_TIMEOUT
    except Exception:
        return False


def _update_device(device_key, serial, ip, extra=None):
    """Atualiza (ou cria) registro do dispositivo no dicionário global."""
    existing = devices.get(device_key, {})
    record = {
        "last_seen":  now_str(),
        "ip":         ip,
        "serial":     serial or existing.get("serial", "desconhecido"),
        "poll_count": existing.get("poll_count", 0) + 1,
        "firmware":   request.headers.get("X-Firmware-Version",
                      existing.get("firmware", "")),
    }
    if extra:
        record.update(extra)
    devices[device_key] = record


def _extract_events_from_body(body, device_key, serial, ip):
    """
    Extrai eventos de acesso do body enviado pelo equipamento.
    O iDFace pode enviar eventos dentro de 'transactions', 'events' ou 'logs'.
    """
    extracted = []
    # Formato 1: {"transactions": [{...}]}
    for tx in body.get("transactions", []):
        # Transação que é evento (tem user_id, event, time)
        if any(k in tx for k in ("user_id", "userId", "event", "type")):
            extracted.append(tx)
    # Formato 2: {"events": [{...}]}
    for ev in body.get("events", []):
        extracted.append(ev)
    # Formato 3: {"logs": [{...}]}
    for lg in body.get("logs", []):
        extracted.append(lg)
    # Armazena
    for data in extracted:
        events.appendleft({
            "received_at": now_str(),
            "device_id":   device_key,
            "serial":      serial or "desconhecido",
            "ip":          ip,
            "data":        data,
        })
    return len(extracted)


# ── Endpoints do protocolo Push (chamados pelo iDFace) ────────────────────────

@app.route("/push", methods=["GET", "POST"])
def push_poll():
    """
    Endpoint principal do protocolo Push.
    • GET  → apenas busca comandos pendentes
    • POST → envia eventos de acesso + recebe comandos
    """
    device_key, serial, ip = _resolve_device_id(request)

    with lock:
        _update_device(device_key, serial, ip)

        # POST: extrai eventos enviados pelo equipamento
        if request.method == "POST":
            body = request.get_json(force=True, silent=True) or {}
            _extract_events_from_body(body, device_key, serial, ip)

        # Retorna próximo comando da fila (se houver)
        if cmd_queue:
            cmd = cmd_queue.popleft()
            return jsonify(cmd)

    return jsonify({"transactions": []}), 200


@app.route("/result", methods=["POST"])
def push_result():
    """O iDFace posta aqui o resultado de cada comando executado."""
    data      = request.get_json(force=True, silent=True) or {}
    device_key, serial, ip = _resolve_device_id(request)

    with lock:
        results.appendleft({
            "received_at": now_str(),
            "device_id":   device_key,
            "serial":      serial or devices.get(device_key, {}).get("serial", "desconhecido"),
            "ip":          ip,
            "data":        data,
        })
        if device_key in devices:
            devices[device_key]["last_seen"] = now_str()

    return jsonify({"success": True}), 200


@app.route("/event", methods=["POST"])
def push_event():
    """
    Endpoint alternativo para eventos de acesso.
    Útil quando o iDFace está configurado para enviar identificações
    separadamente dos polls de comando.
    """
    data      = request.get_json(force=True, silent=True) or {}
    device_key, serial, ip = _resolve_device_id(request)

    with lock:
        events.appendleft({
            "received_at": now_str(),
            "device_id":   device_key,
            "serial":      serial or devices.get(device_key, {}).get("serial", "desconhecido"),
            "ip":          ip,
            "data":        data,
        })
        if device_key in devices:
            devices[device_key]["last_seen"] = now_str()
        else:
            _update_device(device_key, serial, ip)

    return jsonify({"success": True}), 200


# ── Endpoints internos (chamados pelo controlid.py) ───────────────────────────

@app.route("/health", methods=["GET"])
def health():
    with lock:
        online_count = sum(1 for info in devices.values() if _is_online(info))
    return jsonify({
        "status":          "online",
        "timestamp":       now_str(),
        "pending_cmds":    len(cmd_queue),
        "results_count":   len(results),
        "events_count":    len(events),
        "devices_seen":    len(devices),
        "devices_online":  online_count,
    }), 200


@app.route("/devices", methods=["GET"])
def list_devices():
    if not check_token(request):
        return jsonify({"error": "Unauthorized"}), 401

    with lock:
        seen_serials = set()
        resultado = {}
        for key, info in devices.items():
            serial = info.get("serial", "")
            if serial and serial != "desconhecido":
                if serial in seen_serials:
                    continue
                seen_serials.add(serial)
            info_out = dict(info)
            info_out["online"] = _is_online(info)
            resultado[key] = info_out

    return jsonify({"devices": resultado}), 200


@app.route("/events", methods=["GET"])
def list_events():
    if not check_token(request):
        return jsonify({"error": "Unauthorized"}), 401

    limit = int(request.args.get("limit", 100))
    with lock:
        return jsonify({"events": list(events)[:limit]}), 200


@app.route("/results", methods=["GET"])
def list_results():
    if not check_token(request):
        return jsonify({"error": "Unauthorized"}), 401

    limit = int(request.args.get("limit", 50))
    with lock:
        return jsonify({"results": list(results)[:limit]}), 200


@app.route("/cmd", methods=["POST"])
def enqueue_cmd():
    """
    Enfileira um comando para o iDFace executar no próximo poll.
    Body JSON:
      { "method": "POST", "url": "/execute_actions.fcgi", "body": {...} }
    """
    if not check_token(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Body JSON inválido"}), 400

    cmd_id = str(uuid.uuid4())[:8]
    cmd = {
        "transactions": [
            {
                "id":     cmd_id,
                "method": data.get("method", "POST"),
                "url":    data.get("url", "/"),
                "body":   data.get("body", {}),
            }
        ]
    }
    with lock:
        cmd_queue.append(cmd)

    return jsonify({"success": True, "cmd_id": cmd_id,
                    "queued": len(cmd_queue)}), 200


@app.route("/clear", methods=["POST"])
def clear_results():
    """Limpa fila de comandos e histórico de resultados."""
    if not check_token(request):
        return jsonify({"error": "Unauthorized"}), 401

    with lock:
        results.clear()
        cmd_queue.clear()

    return jsonify({"success": True}), 200


@app.route("/clear_events", methods=["POST"])
def clear_events_route():
    """Limpa histórico de eventos de acesso."""
    if not check_token(request):
        return jsonify({"error": "Unauthorized"}), 401

    with lock:
        events.clear()

    return jsonify({"success": True}), 200


# ── Inicialização ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"[Push Server v2.0] Porta: {port}")
    print(f"[Push Server v2.0] Token: {ACCESS_TOKEN}")
    print(f"[Push Server v2.0] Timeout offline: {OFFLINE_TIMEOUT}s")
    app.run(host="0.0.0.0", port=port)
