"""
Servidor Push - Control iD iDFace
Protocolo compatível com o modo Push da API Control iD.

O equipamento faz GET /push periodicamente buscando comandos.
Comandos são enfileirados via API REST (porta 8000).
Resultados chegam via POST /result do próprio equipamento.

Endpoints internos (para o controlid.py chamar):
  POST /cmd          — enfileira um comando para o equipamento
  GET  /results      — lista resultados recebidos
  GET  /devices      — lista dispositivos conectados
  GET  /health       — status do servidor
  POST /clear        — limpa fila de resultados

Endpoints do protocolo Push (para o iDFace):
  GET  /push         — equipamento busca comandos
  POST /result       — equipamento devolve resultado
"""

from flask import Flask, request, jsonify
from collections import deque
from datetime import datetime
import threading
import uuid
import os

app = Flask(__name__)

# ── Estado global (thread-safe) ──────────────────────────────────────────────
lock           = threading.Lock()
cmd_queue      = deque()          # comandos aguardando execução
results        = deque(maxlen=500) # resultados recebidos (máx 500)
devices        = {}               # {device_id: {last_seen, ip, info}}
ACCESS_TOKEN   = os.environ.get("ACCESS_TOKEN", "minha_chave_secreta")

# ── Helpers ──────────────────────────────────────────────────────────────────

def now_str():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def check_token(req):
    token = req.headers.get("X-Access-Token") or req.args.get("token")
    return token == ACCESS_TOKEN

# ── Endpoints do protocolo Push (chamados pelo iDFace) ───────────────────────

def _resolve_device_id(req):
    """
    Monta um identificador único e estável para o dispositivo.
    Prioridade:
      1. Header X-Device-Serial  (enviado pelo controlid.py ao configurar o Push)
      2. Query param serial
      3. Header X-Device-Id
      4. Query param device_id
      5. IP remoto (fallback — pode variar por NAT)
    """
    serial = (req.headers.get("X-Device-Serial")
              or req.args.get("serial", "").strip())
    ip     = req.remote_addr

    if serial:
        return f"{serial}@{ip}", serial, ip

    dev_id = (req.headers.get("X-Device-Id")
              or req.args.get("device_id", "").strip()
              or ip)
    return dev_id, None, ip


@app.route("/push", methods=["GET"])
def push_poll():
    """
    O iDFace chama este endpoint periodicamente.
    Retornamos o próximo comando da fila, ou vazio se não houver.
    """
    device_key, serial, ip = _resolve_device_id(request)
    with lock:
        # Atualiza ou cria registro do dispositivo
        existing = devices.get(device_key, {})
        devices[device_key] = {
            "last_seen":    now_str(),
            "ip":           ip,
            "serial":       serial or existing.get("serial", "desconhecido"),
            "poll_count":   existing.get("poll_count", 0) + 1,
            "firmware":     request.headers.get("X-Firmware-Version",
                            existing.get("firmware", "")),
        }
        if cmd_queue:
            cmd = cmd_queue.popleft()
            return jsonify(cmd)

    return jsonify({"transactions": []}), 200


@app.route("/result", methods=["POST"])
def push_result():
    """
    O iDFace posta aqui o resultado de cada comando executado.
    """
    data      = request.get_json(force=True, silent=True) or {}
    device_key, serial, ip = _resolve_device_id(request)
    with lock:
        results.appendleft({
            "received_at": now_str(),
            "device_id":   device_key,
            "serial":      serial or "desconhecido",
            "ip":          ip,
            "data":        data,
        })
        if device_key in devices:
            devices[device_key]["last_seen"] = now_str()
    return jsonify({"success": True}), 200


# ── Endpoints internos (chamados pelo controlid.py) ───────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":       "online",
        "timestamp":    now_str(),
        "pending_cmds": len(cmd_queue),
        "results_count":len(results),
        "devices_seen": len(devices),
    }), 200


@app.route("/devices", methods=["GET"])
def list_devices():
    if not check_token(request):
        return jsonify({"error": "Unauthorized"}), 401
    with lock:
        # Agrupa por serial se disponível, evitando duplicatas por IP
        seen_serials = set()
        resultado = {}
        for key, info in devices.items():
            serial = info.get("serial", "")
            if serial and serial != "desconhecido":
                if serial in seen_serials:
                    continue
                seen_serials.add(serial)
            resultado[key] = info
        return jsonify({"devices": resultado}), 200


@app.route("/cmd", methods=["POST"])
def enqueue_cmd():
    """
    Enfileira um comando para o iDFace executar.
    Corpo JSON exemplo:
      {"action": "open_door", "parameters": "door=1"}
    ou qualquer payload compatível com o protocolo Push da Control iD.
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
                "id":      cmd_id,
                "method":  data.get("method", "POST"),
                "url":     data.get("url", "/"),
                "body":    data.get("body", {}),
            }
        ]
    }
    with lock:
        cmd_queue.append(cmd)

    return jsonify({"success": True, "cmd_id": cmd_id,
                    "queued": len(cmd_queue)}), 200


@app.route("/results", methods=["GET"])
def list_results():
    if not check_token(request):
        return jsonify({"error": "Unauthorized"}), 401
    limit = int(request.args.get("limit", 50))
    with lock:
        return jsonify({"results": list(results)[:limit]}), 200


@app.route("/clear", methods=["POST"])
def clear_results():
    if not check_token(request):
        return jsonify({"error": "Unauthorized"}), 401
    with lock:
        results.clear()
        cmd_queue.clear()
    return jsonify({"success": True}), 200


# ── Inicialização ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"[Push Server] Rodando na porta {port}")
    print(f"[Push Server] Token de acesso: {ACCESS_TOKEN}")
    app.run(host="0.0.0.0", port=port)
