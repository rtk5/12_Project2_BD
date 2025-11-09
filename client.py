import socket
import json
import os
import hashlib
import struct
import logging
import threading
import time
from flask import Flask, render_template_string, request, jsonify
from threading import Thread

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | CLIENT | %(message)s",
)
log = logging.getLogger("client")

# --- Load configuration file ---
with open("config.json") as f:
    CONFIG = json.load(f)

NAMENODE_HOST = CONFIG["namenode"]["host"]
NAMENODE_PORT = CONFIG["namenode"]["client_port"]
CHUNK_SIZE = int(CONFIG["chunk_size_mb"]) * 1024 * 1024  # bytes

# --- Framed JSON helpers ---
def send_json(sock, obj, who="namenode"):
    data = json.dumps(obj).encode("utf-8")
    header = struct.pack(">I", len(data))
    sock.sendall(header + data)
    log.debug(f"[SEND → {who}] {len(data)} bytes | {obj}")

def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

def recv_json(sock, who="namenode"):
    hdr = recv_exact(sock, 4)
    if not hdr:
        return None
    (length,) = struct.unpack(">I", hdr)
    body = recv_exact(sock, length)
    if not body:
        return None
    obj = json.loads(body.decode("utf-8"))
    log.debug(f"[RECV ← {who}] {obj}")
    return obj

def send_to_namenode(message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        log.info(f"Connecting to Namenode {NAMENODE_HOST}:{NAMENODE_PORT} ...")
        s.connect((NAMENODE_HOST, NAMENODE_PORT))
        send_json(s, message, who="namenode")
        return recv_json(s, who="namenode")
    except Exception as e:
        log.error(f"Error communicating with Namenode: {e}")
        return None
    finally:
        s.close()

# --- Upload helpers ---
def split_file(filename):
    chunks, checksums = [], []
    with open(filename, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            chunks.append(chunk)
            checksums.append(hashlib.md5(chunk).hexdigest())
    return chunks, checksums

def send_chunk(target_host, target_port, chunk_name, data, chunk_index, total_chunks):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        log.info(f"Uploading chunk {chunk_index+1}/{total_chunks} ({chunk_name}) to {target_host}:{target_port}")
        s.connect((target_host, target_port))
        s.sendall(b"STORE\n")
        header = {"chunk_name": chunk_name, "size": len(data)}
        payload = json.dumps(header).encode("utf-8")
        s.sendall(struct.pack(">I", len(payload)) + payload)
        ack = s.recv(32)
        if ack != b"READY":
            log.error(f"Datanode didn't acknowledge READY for {chunk_name}")
            return
        s.sendall(data)
        log.info(f"✅ Sent {chunk_name} successfully")
    except Exception as e:
        log.error(f"Error sending chunk {chunk_name}: {e}")
    finally:
        s.close()

def upload_file(filename):
    if not os.path.exists(filename):
        log.error(f"File not found: {filename}")
        return
    chunks, checksums = split_file(filename)
    num_chunks = len(chunks)
    log.info(f"Split into {num_chunks} chunks")

    resp = send_to_namenode({"action": "upload_request", "filename": os.path.basename(filename), "num_chunks": num_chunks})
    if not resp or resp.get("status") != "ok":
        log.error("Upload request failed.")
        return

    plan = resp["plan"]
    for i, chunk_info in enumerate(plan):
        data = chunks[i]
        for host, port in chunk_info["datanodes"]:
            send_chunk(host, port, chunk_info["chunk_name"], data, i, num_chunks)

    commit = send_to_namenode({"action": "commit_upload", "filename": os.path.basename(filename)})
    if commit and commit.get("status") == "ok":
        log.info("✅ Upload complete and committed.")
    else:
        log.warning("Upload complete but commit not confirmed.")

# --- Download helpers ---
def download_file(filename, output_path):
    meta = send_to_namenode({"action": "download_request", "filename": filename})
    if not meta or meta.get("status") != "ok":
        log.error("Download request failed.")
        return
    chunks_meta = meta["metadata"]
    with open(output_path, "wb") as out:
        for chunk in chunks_meta:
            host, port = chunk["datanodes"][0]
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((host, port))
                s.sendall(b"GET\n")
                header = {"chunk_name": chunk["chunk_name"]}
                data = json.dumps(header).encode("utf-8")
                s.sendall(struct.pack(">I", len(data)) + data)
                buf = b""
                while True:
                    packet = s.recv(65536)
                    if not packet:
                        break
                    buf += packet
                out.write(buf)
            finally:
                s.close()
    log.info(f"✅ File reconstructed as {output_path}")

# --- System status ---
def get_system_status():
    status = send_to_namenode({"action": "system_status"})
    if not status:
        return "⚠️ Unable to retrieve system status."
    lines = []
    lines.append("=== SYSTEM STATUS ===")
    if "nodes" in status:
        lines.append("Healthy Datanodes:")
        for dn, info in status["nodes"].items():
            state = "🟢" if info.get("alive") else "🔴"
            lines.append(f"  {state} {dn} ({info.get('host')}:{info.get('port')})")
    if "files" in status:
        lines.append("\n=== FILE DISTRIBUTION ===")
        for fname, chunks in status["files"].items():
            lines.append(f"{fname}:")
            for ch in chunks:
                lines.append(f"  {ch['chunk_name']} -> {ch['datanodes']}")
    if "integrity" in status:
        lines.append("\n=== FILE INTEGRITY ===")
        for f, res in status["integrity"].items():
            ok = "✅" if res else "❌"
            lines.append(f"  {ok} {f}")
    return "\n".join(lines)

# --- Flask Dashboard ---
app = Flask(__name__)
LOG_BUFFER = []

class LogCaptureHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        LOG_BUFFER.append(msg)
        if len(LOG_BUFFER) > 200:
            LOG_BUFFER.pop(0)
log.addHandler(LogCaptureHandler())

HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
  <title>Storage Client Dashboard</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-dark text-light p-4">
  <h1>📦 Storage Client Dashboard</h1>

  <form id="uploadForm" enctype="multipart/form-data">
    <input type="file" name="file" class="form-control my-2" required>
    <button type="button" class="btn btn-success w-100" onclick="uploadFile()">Upload</button>
  </form>

  <form id="downloadForm" class="mt-3">
    <input type="text" name="filename" placeholder="Filename to download" class="form-control my-2" required>
    <button type="button" class="btn btn-primary w-100" onclick="downloadFile()">Download</button>
  </form>

  <pre id="logArea" class="bg-black text-success mt-3 p-3" style="height:400px; overflow:auto; white-space:pre-wrap;"></pre>

  <script>
    async function apiRequest(path, opts){
      const res = await fetch(path, opts);
      if (!res.ok) throw new Error('Network error');
      return res;
    }

    async function uploadFile(){
      const form = document.getElementById("uploadForm");
      const fd = new FormData(form);
      const res = await apiRequest('/upload', {method:'POST', body:fd});
      const msg = await res.json();
      alert(msg.message);
    }

    async function downloadFile(){
      const form = document.getElementById("downloadForm");
      const fd = new FormData(form);
      const res = await apiRequest('/download', {method:'POST', body:fd});
      const msg = await res.json();
      alert(msg.message);
    }

    async function fetchLogs(){
      try{
        const [logsRes, statusRes] = await Promise.all([
          fetch('/logs'),
          fetch('/status')
        ]);
        const logs = await logsRes.text();
        const status = await statusRes.text();
        const logArea = document.getElementById('logArea');
        logArea.textContent = logs + "\\n\\n" + status;
        logArea.scrollTop = logArea.scrollHeight;
      }catch(e){
        document.getElementById('logArea').textContent = 'Unable to fetch logs/status: '+e;
      }
    }

    setInterval(fetchLogs, 3000);
    fetchLogs();
  </script>
</body>
</html>
"""

@app.route("/")
def dashboard():
    return render_template_string(HTML_PAGE)

@app.route("/upload", methods=["POST"])
def api_upload():
    file = request.files["file"]
    path = os.path.join(".", file.filename)
    file.save(path)
    Thread(target=upload_file, args=(path,), daemon=True).start()
    return jsonify({"message": f"Uploading {file.filename}..."})

@app.route("/download", methods=["POST"])
def api_download():
    filename = request.form.get("filename")
    output_path = f"reconstructed_{filename}"
    Thread(target=download_file, args=(filename, output_path), daemon=True).start()
    return jsonify({"message": f"Downloading {filename}..."})

@app.route("/logs")
def api_logs():
    return "\n".join(LOG_BUFFER[-100:])

@app.route("/status")
def api_status():
    return get_system_status()

def start_dashboard():
    app.run(host="0.0.0.0", port=8080, debug=False)

# --- Main driver ---
if __name__ == "__main__":
    Thread(target=start_dashboard, daemon=True).start()
    print("🌐 Dashboard running at http://localhost:8080")
    while True:
        time.sleep(1)
