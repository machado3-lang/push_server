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

@app.route("/push", methods=["GET"])
def push_poll():
    """
    O iDFace chama este endpoint periodicamente.
    Retornamos o próximo comando da fila, ou vazio se não houver.
    """
    device_id = request.args.get("device_id", request.remote_addr)
    with lock:
        devices[device_id] = {
            "last_seen": now_str(),
            "ip":        request.remote_addr,
        }
        if cmd_queue:
            cmd = cmd_queue.popleft()
            return jsonify(cmd)

    # Sem comandos — retorna resposta vazia no formato esperado pelo firmware
    return jsonify({"transactions": []}), 200


@app.route("/result", methods=["POST"])
def push_result():
    """
    O iDFace posta aqui o resultado de cada comando executado.
    """
    data      = request.get_json(force=True, silent=True) or {}
    device_id = request.args.get("device_id", request.remote_addr)
    with lock:
        results.appendleft({
            "received_at": now_str(),
            "device_id":   device_id,
            "data":        data,
        })
        # Atualiza last_seen do dispositivo
        if device_id in devices:
            devices[device_id]["last_seen"] = now_str()
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
        return jsonify({"devices": dict(devices)}), 200


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
