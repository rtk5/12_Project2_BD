# 🔍 Code Walkthrough — Every File, Every Function

> **What this document is for:** preparing for an interview on this project.
> It explains *all* the code in detail — what every function does, why it is written
> that way, and the concepts it demonstrates.
>
> For the architecture, pipeline and HDFS comparison see [`explanation.md`](explanation.md).

The project is 4 Python programs + config files:

```
Namenode/namenode.py      ~1500 lines  (master: metadata, placement, heartbeats, healer)
DATANODE0/datanode0.py     ~150 lines  (storage node, compact implementation)
Datanode1/datanode1.py     ~1000 lines (storage node, same job, more defensive)
Client/client.py           ~1650 lines (chunking, transfer, Flask dashboard)
```

Only **one** third-party dependency exists in the whole project: **Flask**
(`Client/client.py`). Everything else — `socket`, `threading`, `json`, `struct`,
`hashlib`, `logging`, `os`, `time` — is the Python standard library.

---

# 1. `Namenode/namenode.py` — the master

## 1.1 Imports & logging

```python
import socket, threading, json, time, struct, os, logging
from typing import Dict, List, Tuple, Set
```

- `socket` — raw TCP/UDP servers and clients.
- `threading` — one thread per client connection + background monitor threads.
- `json` — message serialization.
- `struct` — packing the 4-byte length header (big-endian integers).
- `logging` — structured logs with timestamp/level/thread:
  `%(asctime)s | %(levelname)s | %(threadName)s | %(message)s`.

**Interview point:** why logging instead of `print`? — timestamps, severity levels,
per-thread identification, and the ability to attach extra handlers (the client does
exactly that to feed the dashboard).

## 1.2 Configuration loading

```python
def load_config():
    with open("config.json") as f:
        return json.load(f)

config = load_config()
HOST            = config["namenode"]["host"]
CLIENT_PORT     = int(config["namenode"]["client_port"])      # TCP 5000
HEARTBEAT_PORT  = int(config["namenode"]["heartbeat_port"])   # UDP 5001
DATANODES       = config["datanodes"]                          # {"dn0":{host,port,...}, "dn1":...}
REPLICATION     = int(config["replication_factor"])            # 2
HEARTBEAT_TIMEOUT = float(config["heartbeat_timeout_sec"])     # 10 s
CHUNK_BYTES     = config.get("chunk_size_mb", 2) * 1024 * 1024 # 2 MB
```

Everything tunable (ports, nodes, replication factor, chunk size, timeouts) lives in
`config.json`, so the same code runs on the 4-VM lab or on one `localhost` machine.
Note `config.get("chunk_size_mb", 2)` — the `.get` default is a defensive touch in
case the key is missing.

## 1.3 Metadata persistence

```python
METADATA_FILE = "metadata.json"

def save_metadata(metadata):      # dict -> JSON, indent=2, try/except + log
def load_metadata():              # JSON -> dict, or {} if absent/unreadable
```

- Called on every upload, commit and healing pass.
- Errors are **logged, not raised** — a failed metadata write must not crash the
  master (worst case: lose the last update, the in-memory copy survives).

**Interview point — how does real HDFS do this?** An in-memory INode tree +
`fsimage` (snapshot) + `edits` log (append-only journal) + periodic checkpoints by a
secondary NameNode. Our version: a single JSON file rewritten on mutation. Same
principle (durability + restart recovery), far less machinery.

## 1.4 Global shared state

```python
metadata: Dict[str, List[Dict]] = load_metadata()
# filename -> [ {chunk_id, chunk_name, replicas: [[host,port], ...], size}, ... ]

chunk_locations: Dict[Tuple[str, int], Set[str]] = {}
# (filename, chunk_id) -> set of dn_ids that REPORTED holding it (block reports)

heartbeat_table: Dict[str, float] = {dn: 0.0 for dn in DATANODES}
# dn_id -> timestamp of last heartbeat

state_lock = threading.Lock()
```

**Why the lock (say this in an interview):** the NameNode is multithreaded — one
thread per client plus monitor/healer threads. `metadata`, `chunk_locations` and
`heartbeat_table` are shared mutable objects. Without `state_lock`, e.g. a
`metadata[filename] = [...]` from an upload thread could interleave with a read in
`handle_client` or a mutation in `replication_healer` → torn reads, lost updates.

**Design choice:** one coarse lock around the whole critical section (simpler and
deadlock-safe: it's the only lock, so it can't be acquired twice on one thread in the
wrong order). The cost is that one slow client can block others — acceptable at this
scale; a real system would shard by file or use finer-grained/RCU-like structures.

**Initial heartbeat timestamp `0.0`:** at startup `(now - 0)` is huge, so every node
is "dead" until its first real heartbeat arrives. That's a deliberate safe default.

## 1.5 Network helpers — framing (the single most-asked area)

```python
MAX_JSON_FRAME = 128 * 1024 * 1024   # 128 MB sanity cap

def send_json(conn, obj, who="peer"):
    data   = json.dumps(obj).encode("utf-8")
    header = struct.pack(">I", len(data))   # 4 bytes, big-endian
    conn.sendall(header + data)

def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:          # remote closed -> partial frame
            return None
        buf += chunk
    return buf

def recv_json(conn, who="peer"):
    header = recv_exact(conn, 4)
    if not header: return None
    (length,) = struct.unpack(">I", header)
    if length <= 0 or length > MAX_JSON_FRAME:
        raise ValueError("Invalid frame length")
    body = recv_exact(conn, length)
    return json.loads(body.decode("utf-8"))
```

Key concepts to articulate:

1. **TCP is a byte stream, not a message protocol.** There is no guarantee that
   `send(b"X")` arrives as one `recv()` unit, nor that `recv(n)` returns n bytes.
   So any text-JSON-over-TCP system must implement **framing**: length-prefix
   (`[4-byte length][payload]`) or delimiter-based. We use length-prefix.
2. **Why big-endian (`>`)?** Network byte order — a universal convention
   (RFC 1700) so x86, ARM, little/big-endian hosts all decode the same integer.
   `I` = unsigned 32-bit int, so frames up to ~4 GB are addressable; we self-limit
   to 128 MB.
3. **Why loop in `recv_exact`?** TCP segments can be split (MTU limits, Nagle,
   congestion windows). Loop until the full count is in; `recv() == b""` means the
   peer closed → return `None` (caller treats as disconnect).
4. **`sendall` vs `send`** — `sendall` loops until *everything* is sent (or error).
5. **The size cap** is basic input validation: a corrupt/malicious 4-byte field
   otherwise tells us to allocate arbitrarily much memory.

**Alternative framings (good follow-up answer):** newline-delimited JSON (works only
if payload can't contain `\n` after encoding — JSON escapes it, so it's viable),
or an actual RPC framework (Thrift/protobuf/gRPC) with proper message types.

## 1.6 Liveness helpers

```python
def live_datanodes_ids():
    now = time.time()
    return [dn for dn, last in heartbeat_table.items()
            if (now - last) <= HEARTBEAT_TIMEOUT]

def datanode_host_port(dn_id):
    info = DATANODES[dn_id]
    return info["host"], int(info["port"])
```

Liveness is **computed on demand**, not stored — no "dead flag" to forget clearing.
Note the comparison is `<=` (exactly-at-timeout still counts as alive).

## 1.7 Placement policy

```python
def plan_two_replicas(cid, live_ids) -> List[Tuple[str, int]]:
    if set(live_ids) == {"dn0", "dn1"}:
        primary   = "dn0" if cid % 2 == 0 else "dn1"
        secondary = "dn1" if primary == "dn0" else "dn0"
        return [datanode_host_port(primary), datanode_host_port(secondary)]
    elif "dn0" in live_ids: return [datanode_host_port("dn0")]
    elif "dn1" in live_ids: return [datanode_host_port("dn1")]
    else: return []
```

- Both alive → chunk goes to **both** nodes; the *primary* alternates by chunk id so
  the "first writer" load splits evenly.
- One alive → degraded mode: store the single available replica (availability over
  the replication guarantee; real HDFS would reject or queue instead).
- None alive → refuse (caller aborts the upload).

**Interview point — how would you generalize to N nodes?**
(a) maintain an ordered candidate list (prefer local rack, then other racks, avoid
already-selected nodes), (b) respect per-node disk capacity, (c) real HDFS: the
client proposes DataNodes from its own placement hints, the NameNode finalizes.

## 1.8 `handle_client` — the control-plane dispatcher

```python
def handle_client(conn, addr):
    cname = f"{addr[0]}:{addr[1]}"
    try:
        req = recv_json(conn, who=cname)     # ONE request per connection
        action = req.get("action")
        ...
    except Exception as e:
        log.error(f"[ERROR] {cname}: {e}")
    finally:
        conn.close()
```

One thread per connection, one request-response exchange, then close. That's a
simple, correct RPC style (compare: HTTP/1.0 request-per-connection).

### `upload` / `upload_request`

```python
filename   = req["filename"]
num_chunks = int(req["num_chunks"])
chunk_sizes = req.get("chunk_sizes", [])     # optional
with state_lock:
    live = live_datanodes_ids()
    if not live:  send error "No live datanodes"; return
    plan = []
    for cid in range(num_chunks):
        endpoints = plan_two_replicas(cid, live)
        plan.append({"chunk_id": cid,
                     "chunk_name": f"{filename}.chunk{cid}",
                     "datanodes": endpoints})
    metadata[filename] = [
        {"chunk_id": cid,
         "chunk_name": f"{filename}.chunk{cid}",
         "replicas":  plan[cid]["datanodes"],
         "size": chunk_sizes[cid] if cid < len(chunk_sizes) else CHUNK_BYTES}
        for cid in range(num_chunks)
    ]
    save_metadata(metadata)
send_json(conn, {"status": "ok", "plan": plan})
```

Observations worth mentioning:

- **The client sends only filename + chunk count.** The bytes never touch the
  master — placement is decided centrally, transfer happens edge-to-edge.
- **Re-upload overwrites** `metadata[filename]` (last upload wins).
- Metadata is written *before* the client has pushed data. Consequence: the file is
  "visible" mid-upload. Real HDFS has an analogous subtlety (block registration on
  first write), plus a close() that validates the length.
- `chunk_sizes` is supported by the protocol but the current client doesn't send it,
  so the last (partial) chunk is recorded at full size — a known gap.

### `commit_upload`

Looks the file up under the lock; if present → persist + `ok`; else `unknown file`.
In this implementation the commit is essentially an acknowledgement/audit point.

### `download` / `download_request`

```python
if filename not in metadata:  -> error "File not found"
live = set(live_datanodes_ids())
for rec in metadata[filename]:
    live_repls = [(host, port) for host, port in rec["replicas"]
                  if the (host,port) matches a CONFIGURED dn AND that dn is in live]
    result.append({... "datanodes": live_repls or rec["replicas"], ...})
send_json(conn, {"status": "ok", "metadata": result})
```

Two-layer fallback: prefer **live** replicas; if none are currently live, return the
recorded ones anyway so the client can attempt a connection (a node whose heartbeat
is stale might still answer TCP). That's a deliberate availability choice.

### `list_files`

`list(metadata.keys())` under the lock — trivial, but demonstrates snapshotting the
key set inside the critical section.

### `block_report`

```python
dn_id  = req["dn_id"]
blocks = req.get("blocks", [])
with state_lock:
    for b in blocks:
        key  = (b["filename"], int(b["chunk_id"]))
        refs = chunk_locations.setdefault(key, set())
        refs.add(dn_id)
send_json(conn, {"status": "ok"})
```

`dict.setdefault(key, set())` is the idiomatic "get-or-create" in one call.
This is the intended ground-truth feed for the healer (see §1.11) — implemented on
the receiving side, not yet sent by any component.

### Error handling style

Every handler: `try / except Exception -> log / finally -> conn.close()`. A bad
request can kill its own thread but never the listener or the process.

## 1.9 `client_listener`

```python
s = socket.socket(AF_INET, SOCK_STREAM)
s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
s.bind(("0.0.0.0", CLIENT_PORT)); s.listen()
while True:
    conn, addr = s.accept()
    threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
```

- `SO_REUSEADDR` — lets the port be rebound immediately after a restart instead of
  waiting out the TCP `TIME_WAIT` state.
- `0.0.0.0` — accept on any interface (lab VMs talk over the network).
- `daemon=True` — worker threads don't block process exit; the listener loop is what
  keeps the process alive.

## 1.10 `heartbeat_listener` / `heartbeat_monitor`

```python
def heartbeat_listener():                     # runs in its own thread
    s = socket.socket(AF_INET, SOCK_DGRAM)    # UDP
    s.bind(("0.0.0.0", HEARTBEAT_PORT))
    while True:
        msg, addr = s.recvfrom(1024)
        dn_id = msg.decode().strip()
        with state_lock:
            if dn_id in heartbeat_table:      # ignore unknown senders
                heartbeat_table[dn_id] = time.time()

def heartbeat_monitor():
    while True:
        time.sleep(5)
        with state_lock:
            for dn_id, last in heartbeat_table.items():
                if time.time() - last > HEARTBEAT_TIMEOUT:
                    log.warning(f"[DOWN] {dn_id} missed heartbeats")
                else:
                    log.info(f"[ALIVE] {dn_id}")
```

**Why UDP here and TCP everywhere else?** Heartbeats are small, frequent,
order-insensitive (the latest one is all that matters) and must never queue behind
retransmissions. Loss of one ping costs nothing: the next arrives in 3 s. This is
the classic "UDP for liveness/telemetry, TCP for data" split (same as NTP, DNS,
game traffic).

**Security note to volunteer in interview:** an unauthenticated UDP port means any
host could ping a known ID and keep a node "alive". Real systems authenticate
heartbeats (Kerberos in HDFS).

## 1.11 `replication_healer`

```python
while True:
    time.sleep(10)
    with state_lock:
        live = set(live_datanodes_ids())
        for filename, chunks in metadata.items():
            for rec in chunks:
                holders = chunk_locations.get((filename, rec["chunk_id"]), set())
                if len(holders) < REPLICATION:
                    candidates = [dn for dn in DATANODES
                                  if dn not in holders and dn in live]
                    for dn_id in candidates[:REPLICATION - len(holders)]:
                        rec["replicas"].append(
                            datanode_host_port(dn_id)
                        )
                        log.info(
                            f"[HEAL] {filename} chunk {rec['chunk_id']}"
                            f" -> added {dn_id}"
                        )
        save_metadata(metadata)
```

How it works:
- `holders` = who *actually reported* having this chunk (block reports).
- If `len(holders) < REPLICATION`, pick live, non-holding nodes and append their
  addresses to the chunk's `replicas` list, up to the missing count
  (`REPLICATION - len(holders)`).
- Persists the healed metadata.

**Honest status (say this!):** because no component currently sends block reports,
`holders` is always `∅`, so as written the healer keeps *appending* live nodes to
`replicas` every 10 s (duplicates accumulate in `metadata.json`). The intended
design is: block reports feed `chunk_locations` → healer detects under-replication →
issues a `REPLICATE` request to the new holder (command exists in DN1) → re-check
after the copy. Two missing wirings: (1) DataNode-side block-report sender,
(2) deduplication of the replica list (compare before append).

## 1.12 `main`

```python
if __name__ == "__main__":
    log.info(f"[NAMENODE STARTED] Host={HOST}, ClientPort={CLIENT_PORT}, HBPort={HEARTBEAT_PORT}")
    threading.Thread(target=client_listener,      daemon=True).start()
    threading.Thread(target=heartbeat_listener,   daemon=True).start()
    threading.Thread(target=heartbeat_monitor,    daemon=True).start()
    threading.Thread(target=replication_healer,   daemon=True).start()
    while True:
        time.sleep(60)
```

Four daemon threads (all services) + an idle main loop that just keeps the process
alive. `if __name__ == "__main__"` is the standard "only run when executed directly,
not when imported" guard.

**Concurrency summary for interviews:** 1 lock, 6+ threads (1 per client + 4
services + main). No nested lock acquisition ⇒ no deadlock. GIL is irrelevant here
because the workload is I/O-bound — threads release the GIL while blocked on
`recv`/`sleep`/`accept`, so concurrency comes for free.

---

# 2. `DATANODE0/datanode0.py` — the compact storage node

This is the shortest file and makes a good "explain it line by line" warm-up.

```python
import os, socket, threading, time, json, struct, logging, hashlib
```

## 2.1 Config & logging

```python
def load_config():
    with open("config.json") as f:
        return json.load(f)
config = load_config()

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s")
log = logging.getLogger("datanode0")
```

`DEBUG` level: every network event is logged (great for tracing a distributed system;
you'd drop to `INFO` in production to cut noise).

## 2.2 Helpers

```python
def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        part = conn.recv(n - len(buf))
        if not part:
            log.debug(...)          # connection closed mid-frame
            return None
        buf += part
    return buf

def checksum(data):
    return hashlib.md5(data).hexdigest()
```

Same TCP-stream reasoning as the NameNode's `recv_exact`. `checksum` returns the MD5
hex digest of a byte string — used for integrity *detection*.

## 2.3 `datanode_0(...)` — main function

```python
def datanode_0(node_id, host, port, storage_dir,
               namenode_host, namenode_heartbeat_port):
    os.makedirs(storage_dir, exist_ok=True)
    log.info(f"[START] Datanode0 primary {node_id} at {host}:{port}")
```

`os.makedirs(..., exist_ok=True)` — idempotent directory creation: if it exists,
no error; if not, create (with parents). The parameter list makes the node fully
config-driven — one function could power N datanodes.

### Heartbeat sender (closure over `node_id`, `config`)

```python
def send_heartbeat():
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(node_id.encode(), (namenode_host, namenode_heartbeat_port))
            s.close()
            log.debug(f"[HEARTBEAT] Sent from {node_id} ...")
        except Exception as e:
            log.error(f"[HEARTBEAT_FAIL] {e}")
        time.sleep(config["heartbeat_interval_sec"])
```

- A **new UDP socket every beat** (works fine; a persistent socket would be a tiny
  optimization — UDP sockets are stateless).
- The payload is just the node id string, e.g. `"dn0"`.
- Failure is logged and swallowed — a transient network glitch must not kill a
  storage server; the next beat retries, and the NameNode's 10 s timeout absorbs
  gaps.

### Chunk server (`listen_for_chunks`)

```python
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((host, port)); s.listen()
while True:
    conn, addr = s.accept()
    try:
        # (a) read command: one byte at a time until '\n'
        cmd = b""
        while not cmd.endswith(b"\n"):
            part = conn.recv(1)
            if not part: break
            cmd += part
        cmd = cmd.decode().strip()          # "STORE" / "GET"

        # (b) read framed JSON header: [4-byte BE length][JSON]
        hdr_len = struct.unpack(">I", recv_exact(conn, 4))[0]
        header  = json.loads(recv_exact(conn, hdr_len).decode())

        if cmd == "STORE":
            conn.sendall(b"READY")          # handshake: header accepted
            data = b""
            while len(data) < header["size"]:
                part = conn.recv(header["size"] - len(data))
                if not part: break          # sender closed early
                data += part
            log.info(f"[STORE_CHECKSUM] {header['chunk_name']} MD5={checksum(data)}")
            with open(os.path.join(storage_dir, header["chunk_name"]), "wb") as f:
                f.write(data)

        elif cmd == "GET":
            path = os.path.join(storage_dir, header["chunk_name"])
            if not os.path.isfile(path):
                conn.sendall(b"ERROR:NOT_FOUND"); continue
            data = open(path, "rb").read()
            digest = checksum(data)
            if "checksum" in header and header["checksum"] != digest:
                conn.sendall(b"ERROR:INTEGRITY_FAIL"); continue
            conn.sendall(data)              # client reads until FIN
    except Exception as e:
        log.exception(f"[ERROR] Exception handling client {addr}: {e}")
    finally:
        conn.close()
```

Walk-through of the protocol:

1. **Command line first** (`STORE\n` / `GET\n`): a human-readable verb, read
   byte-by-byte until newline. (Reads of 1 byte are a deliberate teaching choice;
   `makefile()`-style buffered reading would also work.)
2. **Framed JSON header** — the DataNode needs *metadata about the payload*
   (chunk name, size) before bytes arrive; framing solves the same
   message-boundary problem as §1.5.
3. **`READY` handshake** on store: the DataNode confirms it parsed the header and is
   prepared to accept `size` bytes. The client treats a non-`READY` reply as failure.
4. **Exact-size receive loop**: the header promised `size` bytes, so read until
   exactly that many (or the connection dies early → short write logged).
5. **Write-through to disk** in binary mode; the chunk file is named
   `<filename>.chunk<id>` (e.g. `a.txt.chunk0`), matching the NameNode's naming so
   a `GET` can resolve the path directly.
6. **`GET` with optional checksum verification**: if the requester includes a
   `checksum` in the header and it doesn't match the stored data, the DataNode
   refuses to serve (`ERROR:INTEGRITY_FAIL`) rather than send corrupt bytes.
7. **`log.exception(...)`** in the except block logs the full traceback (a common
   interview detail: `exception()` = `error()` + traceback).
8. **`finally: conn.close()`** — connection hygiene regardless of success/failure;
   the listener loop immediately accepts the next client, so one bad peer can't
   wedge the server.

### Wiring

```python
threading.Thread(target=send_heartbeat, daemon=True, name="dn0-heartbeat").start()
listen_for_chunks()          # runs in the MAIN thread (blocks forever)

if __name__ == "__main__":
    cfg = config["datanodes"]["dn0"]; nn = config["namenode"]
    datanode_0("dn0", cfg["host"], cfg["port"], cfg["storage_dir"],
               nn["host"], nn["heartbeat_port"])
```

- `name="dn0-heartbeat"` shows up in the log's `%(threadName)s` field — cheap
  observability.
- The TCP server runs in the main thread (its `while True` never returns), heartbeats
  in a daemon thread. Either way works; daemons just don't block interpreter exit.

**Limitations of this file (interview honesty):** no checksum verification on store
(MD5 is computed and logged only), no `REPLICATE` command, and reads the full payload
in unbounded recv sizes (fine at 2 MB; a production server would read in fixed-size
blocks and write them streaming to disk).

---

# 3. `Datanode1/datanode1.py` — the defensive storage node

Same architecture, same protocol, with stricter verification. Structure mirrors
`datanode0.py` 1:1 (config → logging → helpers → `datanode_1()` with
`send_heartbeat` + `listen_for_chunks` closures → entry point). The differences are
the interesting part:

### 3.1 STORE / REPLICATE with verification

```python
if cmd in ("REPLICATE", "STORE"):
    chunk_name   = header["chunk_name"]
    size         = header["size"]
    checksum_ref = header.get("checksum")        # optional
    conn.sendall(b"READY")

    data = b""
    while len(data) < size:
        block = conn.recv(min(4096, size - len(data)))   # capped reads
        if not block: break
        data += block

    local_sum = checksum(data)
    if checksum_ref and local_sum != checksum_ref:
        log.warning(f"[CHECK_FAIL] {chunk_name} corrupted during transfer!")
        conn.sendall(b"ERROR:CHECKSUM_FAIL")
        continue                                    # DO NOT store corrupt data
    with open(os.path.join(storage_dir, chunk_name), "wb") as f:
        f.write(data)
    log.info(f"[STORE_OK] Stored {chunk_name} ({len(data)} bytes) MD5={local_sum}")
```

Differences vs DN0:

1. **`REPLICATE` is a first-class command** — the hook for the (not yet wired)
   DataNode-to-DataNode re-replication flow.
2. **Capped reads** `recv(min(4096, remaining))` — bounded buffers, idiomatic
   streaming-receive loop (the client does the same with 64 KB).
3. **Checksum on the store path**: if the sender supplied a reference MD5 and it
   doesn't match what was received, the DataNode reports `ERROR:CHECKSUM_FAIL` and
   **refuses to persist** the corrupt payload. Note `header.get("checksum")` —
   optional field handled gracefully; and today the client doesn't send a reference,
   so this is a ready-but-unused safety net.

### 3.2 GET with integrity gate

Identical logic to DN0's GET (missing → `ERROR:NOT_FOUND`; checksum mismatch →
`ERROR:INTEGRITY_FAIL`; otherwise stream the bytes).

### 3.3 Everything else

Heartbeat sender, `recv_exact`, MD5 helper, threading layout, entry point
(`config["datanodes"]["dn1"]`) — identical patterns to DN0.

**Interview question — why two DataNode files instead of one generic `datanode.py`
with a `--id dn1` flag?** Honest answer: for the lab each "node" is its own folder
running on its own machine, and the separate files make the VM deployment explicit
(one script per node, zero arguments). The production answer is one image/script +
config — which is in fact what the code already supports, since `datanode_0/1`
functions take all parameters; the folders are a packaging choice.

---

# 4. `Client/client.py` — chunking, transfer, dashboard

## 4.1 Imports & config

```python
import socket, json, os, hashlib, struct, logging, threading, time
from flask import Flask, render_template_string, request, jsonify
from threading import Thread

NAMENODE_HOST = CONFIG["namenode"]["host"]
NAMENODE_PORT = CONFIG["namenode"]["client_port"]      # 5000
CHUNK_SIZE    = int(CONFIG["chunk_size_mb"]) * 1024 * 1024   # 2 MB
```

The client's view of the cluster is **only the NameNode address**. It never hardcodes
DataNode addresses — it receives them inside the plan. (Its config file contains the
full cluster config for symmetry, but the code only uses the NameNode part.)

## 4.2 Framed JSON helpers

`send_json` / `recv_exact` / `recv_json` — byte-for-byte the same framing scheme as
the NameNode (§1.5). This consistency is important: **one framing rule across the
control plane** means the protocol is trivially testable with `nc` or a 10-line
debug script.

## 4.3 `send_to_namenode(message)`

```python
def send_to_namenode(message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((NAMENODE_HOST, NAMENODE_PORT))
        send_json(s, message, who="namenode")
        return recv_json(s, who="namenode")
    except Exception as e:
        log.error(f"Error communicating with Namenode: {e}")
        return None
    finally:
        s.close()
```

A minimal RPC client: **new connection per call** (stateless), one request, one
response, close. Error contract: `None` on failure → callers check before use.
No retry loop — a known improvement point (retry with backoff, or treat
"connection refused" as "NameNode down, abort politely").

## 4.4 `split_file(filename)`

```python
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
```

- `f.read(n)` on a binary file returns up to *n* bytes; `b""` means EOF. That
  one line implements fixed-size chunking; the final iteration returns the short
  tail chunk.
- 5 MB file, 2 MB chunks → `[2 MB, 2 MB, 1 MB]`.
- MD5 per chunk → integrity reference (see §4.8 for the current usage gap).

**Memory note:** all chunks are held in memory (fine for lab files; a streaming
version would chunk the file *during* the transfer — `read` one chunk, send it,
repeat).

## 4.5 `send_chunk(...)` — one chunk to one DataNode

```python
def send_chunk(target_host, target_port, chunk_name, data, chunk_index, total_chunks):
    s = socket.socket(AF_INET, SOCK_STREAM)
    try:
        s.connect((target_host, target_port))
        s.sendall(b"STORE\n")
        header  = {"chunk_name": chunk_name, "size": len(data)}
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
```

The complete store handshake: verb → framed header → `READY` ack → raw bytes.
Failure of one (host, chunk) pair is **logged, not fatal** to the whole upload —
the loop continues with the next target (no automatic retry of the failed leg;
that's where replication + the healer should step in).

## 4.6 `upload_file(filename)` — orchestration

```python
if not os.path.exists(filename): log.error(...); return
chunks, checksums = split_file(filename)
num_chunks = len(chunks)

resp = send_to_namenode({"action": "upload_request",
                         "filename": os.path.basename(filename),
                         "num_chunks": num_chunks})
if not resp or resp.get("status") != "ok": log.error("Upload request failed."); return
plan = resp["plan"]

for i, chunk_info in enumerate(plan):
    data = chunks[i]
    for host, port in chunk_info["datanodes"]:
        send_chunk(host, port, chunk_info["chunk_name"], data, i, num_chunks)

commit = send_to_namenode({"action": "commit_upload",
                           "filename": os.path.basename(filename)})
...
```

The full control flow: validate → chunk → **ask the master for the plan** →
**stream each chunk to every assigned replica** → **commit**. Note the client
deliberately does *not* decide placement — the master does (that separation is the
whole point of the architecture). `os.path.basename` strips any local directory part
so the stored name is just the filename.

## 4.7 `download_file(filename, output_path)`

```python
meta = send_to_namenode({"action": "download_request", "filename": filename})
if not meta or meta.get("status") != "ok": ...; return
chunks_meta = meta["metadata"]

with open(output_path, "wb") as out:
    for chunk in chunks_meta:
        host, port = chunk["datanodes"][0]      # first listed replica
        s = socket.socket(AF_INET, SOCK_STREAM)
        try:
            s.connect((host, port))
            s.sendall(b"GET\n")
            data = json.dumps({"chunk_name": chunk["chunk_name"]}).encode()
            s.sendall(struct.pack(">I", len(data)) + data)
            buf = b""
            while True:
                packet = s.recv(65536)          # 64 KB reads
                if not packet: break            # DataNode closed -> chunk complete
                buf += packet
            out.write(buf)
        finally:
            s.close()
log.info(f"✅ File reconstructed as {output_path}")
```

- Chunks are requested **in metadata order** and appended in order → byte-exact
  reconstruction (concatenation property of fixed-boundary chunking).
- Completion is detected by **connection close** (FIN → empty `recv`), because the
  GET response has no explicit length framing on the DataNode side.
- **Known simplifications to state out loud:** only replica `[0]` is attempted (no
  failover to the second address), no size verification against the planned
  `size`, and a mid-download exception aborts the remaining chunks (partial file
  left behind).

## 4.8 `get_system_status()`

Sends `{"action": "system_status"}` and formats the expected response:
`nodes` (alive booleans → 🟢/🔴), `files` (per-chunk DataNode lists), `integrity`
(✅/❌ per file). **The NameNode doesn't implement that action yet** (it answers
`unknown action`), so the dashboard's status pane currently shows only the header
line — a clean, small follow-up task (implement the action in `handle_client` by
combining `heartbeat_table` + `metadata`).

## 4.9 Flask dashboard

### Log capture

```python
LOG_BUFFER = []

class LogCaptureHandler(logging.Handler):
    def emit(self, record):
        LOG_BUFFER.append(self.format(record))
        if len(LOG_BUFFER) > 200:
            LOG_BUFFER.pop(0)

log.addHandler(LogCaptureHandler())
```

A custom `logging.Handler` subclasses `emit()` — the standard hook — to mirror every
client log line into a **rolling in-memory buffer** (capped at 200, exposing the
latest 100 via `/logs`). This is how the terminal logs reach the browser without
touching the rest of the code.

### HTML page (`HTML_PAGE`, rendered with `render_template_string`)

- One inline HTML string: Bootstrap 5 via CDN, dark theme.
- **Upload form** → `FormData` → `POST /upload`; **Download form** → `POST /download`
  (the buttons use JS `fetch` + `alert(msg.message)` because the real work is
  asynchronous — the server responds instantly and the job runs in a thread).
- **Live view:** `setInterval(fetchLogs, 3000)`; `fetchLogs()` uses
  `Promise.all([fetch('/logs'), fetch('/status')])` to grab both in parallel, then
  renders them into a `<pre>` and auto-scrolls to the bottom.
  (Classic "poll the server" dashboard; the natural upgrade is WebSocket/SSE push —
  listed under future work.)

### Routes

| Route | Method | What it does |
|-------|--------|--------------|
| `/` | GET | `render_template_string(HTML_PAGE)` |
| `/upload` | POST | saves upload to `./<filename>`; `Thread(target=upload_file, args=(path,), daemon=True).start()`; returns `{message: "Uploading ..."}` immediately |
| `/download` | POST | `Thread(target=download_file, args=(filename, f"reconstructed_{filename}"))`; immediate JSON reply |
| `/logs` | GET | `"\n".join(LOG_BUFFER[-100:])` |
| `/status` | GET | `get_system_status()` as plain text |

**Why background threads per job?** Flask's dev server would otherwise block the
whole UI for the duration of a multi-MB multi-DataNode transfer. The HTTP response
is a *job ticket*; progress shows up in the log pane. (Production answer: task queue
+ job IDs + progress polling; or `stream_with_context`.)

### `start_dashboard()` & main

```python
app.run(host="0.0.0.0", port=8080, debug=False)
```

- `0.0.0.0` so lab VMs/users can reach it over the network; `debug=False` so the
  Werkzeug debugger isn't exposed (security 101 for Flask).
- Main thread: start the dashboard in a daemon thread, print the URL, then
  `while True: time.sleep(1)` to keep the process alive.

**Flask interview points:** `request.files` (Werkzeug file storage),
`request.form` (URL-encoded fields), `jsonify`, `render_template_string` vs
`render_template` (inline string vs template file), why the built-in server is fine
for a dashboard demo but not for production (use gunicorn/uWSGI), and how Flask
handles concurrent requests (threaded mode by default in recent versions).

---

# 5. `config.json` (5 copies — root + one per component)

```json
{
  "namenode": { "host": "192.168.191.205", "client_port": 5000, "heartbeat_port": 5001 },
  "datanodes": {
    "dn0": { "host": "192.168.191.47", "port": 6001, "storage_dir": "./storage_dn0" },
    "dn1": { "host": "192.168.191.86", "port": 6002, "storage_dir": "./storage_dn1" }
  },
  "replication_factor": 2,
  "chunk_size_mb": 2,
  "heartbeat_interval_sec": 3,
  "heartbeat_timeout_sec": 10
}
```

- The committed values are from the 4-VM lab (each VM runs one component with its
  own IP). **For a single-machine run, set all hosts to `localhost` in all four
  component configs** (the root copy is informational).
- Each component reads only the part it needs: the NameNode reads everything,
  DataNodes read `datanodes.dnX` + `namenode`, the client reads `namenode` +
  `chunk_size_mb`.
- Tunable knobs → the same binary runs in any topology. (Interview: "why not
  environment variables?" — either works; a JSON file is human-editable per node
  and version-controllable.)

# 6. `Namenode/metadata.json`

The persisted namespace. Empty (`{}`) in a fresh clone. After uploading `a.txt`
(5 MB) it looks like:

```json
{
  "a.txt": [
    { "chunk_id": 0, "chunk_name": "a.txt.chunk0",
      "replicas": [["192.168.191.47", 6001], ["192.168.191.86", 6002]],
      "size": 2097152 },
    { "chunk_id": 1, "chunk_name": "a.txt.chunk1",
      "replicas": [["192.168.191.86", 6002], ["192.168.191.47", 6001]],
      "size": 2097152 },
    { "chunk_id": 2, "chunk_name": "a.txt.chunk2",
      "replicas": [["192.168.191.47", 6001], ["192.168.191.86", 6002]],
      "size": 2097152 }
  ]
}
```

This single file is the entire "database" of the master — the equivalent (in
spirit) of HDFS's `fsimage`.

---

# 7. Concept checklist (map code → CS concept)

| Concept | Where it lives |
|---------|----------------|
| Master/worker (metadata vs data) separation | NameNode never stores bytes; DNs never know the whole file |
| Message framing over a byte stream | `send_json`/`recv_json`/`recv_exact` (all three control files); command+length+header+payload on DNs |
| Network byte order | `struct.pack/unpack(">I", ...)` |
| Reliable data transfer | `sendall`, exact-size receive loops, `READY` handshake |
| Liveness detection | UDP heartbeats (3 s) + timeout (10 s) + derived `live_datanodes_ids()` |
| Replication for fault tolerance | `plan_two_replicas`, client fan-out store, RF=2 |
| Checksums / integrity | MD5 per chunk; verify-on-store (DN1), verify-on-get (both) |
| Concurrency: thread-per-connection | NameNode listener, Flask worker threads, per-job upload/download threads |
| Shared state + locking | `state_lock` around `metadata`/`chunk_locations`/`heartbeat_table` |
| GIL & I/O-bound parallelism | All four processes are I/O-bound; threads are the right tool |
| Daemon threads & process lifetime | Every background thread is `daemon=True`; main loops keep processes alive |
| `SO_REUSEADDR` / `TIME_WAIT` | All TCP servers |
| State persistence / recovery | `metadata.json` saved on mutation, loaded at startup |
| Graceful degradation | One DN alive → upload on the survivor; stale replica list → client still tries |
| Custom logging handler → UI | `LogCaptureHandler` → `/logs` → dashboard |
| Background job pattern in a web app | `/upload`, `/download` return immediately; work in `Thread` |
| Rolling buffer | `LOG_BUFFER` capped at 200, exposes 100 |
| Input validation | `MAX_JSON_FRAME` cap, unknown-action error, `dn_id in heartbeat_table` check |
| Client/server API design | action-based JSON RPC (`upload_request`, `download_request`, `commit_upload`, `block_report`, `list_files`) |

---

# 8. Interview Q&A you should be ready for

**Q: Why 4-byte big-endian length framing? What breaks without it?**
TCP is a stream — messages concatenate on the wire. Without framing, two JSON
objects sent back-to-back arrive as one blob and you can't tell where one ends.
Big-endian = network byte order so every architecture decodes the same length.
`recv()` also never guarantees returning the full requested count, hence
`recv_exact`'s loop.

**Q: Why UDP for heartbeats but TCP for everything else?**
Heartbeats are tiny, frequent, and only the *latest* matters. UDP has no
retransmission, no ordering, no connection setup — a lost ping is free (the next
arrives in 3 s), and the 10 s timeout absorbs bursts. Data transfer needs
guaranteed in-order delivery → TCP. Same reasoning as NTP/DNS vs HTTP.

**Q: Why MD5?**
Fast and adequate for *accidental* corruption detection (bit flips, disk errors).
It's cryptographically broken (collision attacks) and would be unacceptable for
*adversarial* integrity — that's what real HDFS's per-sub-block CRC32 is also
about: speed. If asked: "we'd move to CRC32C or SHA-256 for anything security-related."

**Q: Walk through what happens when a DataNode dies mid-upload.**
Heartbeats stop → after 10 s it leaves `live_datanodes_ids()` → any *new* uploads
place only on survivors; existing `replicas` metadata still lists the dead node, so
download returns that address (with the fallback to recorded replicas) and the
client's connection fails — today the file transfer is aborted (no read failover).
The healer would add fresh replica slots but nothing physically copies the missing
bytes yet (no block reports, no DN→DN `REPLICATE`). If I extended it: block reports
on startup, healer issues `REPLICATE` to a live node, which `GET`s the chunk from a
surviving replica — the data path real HDFS uses.

**Q: Where could this system deadlock or race?**
One lock, acquired once per critical section, never re-entered ⇒ no deadlock.
Without the lock: two uploads interleaving `metadata[f] = [...]` with a download's
iteration would race; the monitor/healer reading while a write mutates could tear
dict iteration. I kept the lock *coarse* (whole section) deliberately — simpler and
correct; the cost is serialized control-plane operations, which are millisecond-scale.

**Q: Why not just use HTTP for the whole protocol?**
You could (and it'd be more standard); we used raw sockets to demonstrate the
underlying mechanics — framing, byte order, stream semantics, handshakes. The
control messages are JSON anyway, so swapping the transport for HTTP/REST would be
a small change; the chunk data plane (length-prefixed binary with a `READY`
handshake) is deliberately not HTTP because it's a dumb, fast, framed byte push.

**Q: Thread-per-connection — does it scale?**
Threads are cheap in CPython for I/O-bound work (GIL released during syscalls) but
memory-grows with connections (8 MB default stack each); 10k connections ≈ heavy.
Scaling options: `selectors`/asyncio event loop, thread pools with bounded
workers, or an RPC framework. At lab scale, thread-per-connection is the clearest
correct model.

**Q: What's in `metadata.json` and why does the NameNode write it on every upload?**
File → ordered chunk list → replica (host, port) + size. Persisting on mutation
makes the master *restart-safe*: kill the NameNode, restart, the namespace is
loaded back (`load_metadata`) and downloads keep working. It's our fsimage
equivalent (minus the edit-log/checkpoint machinery).

**Q: How would you add NameNode HA?**
Standby NN + replicated/journalled metadata (HDFS uses a JournalNode quorum — QJM —
or ZooKeeper — KJM; the active and standby share the edit log), a fencing mechanism
so only one master issues commands, and a health monitor + failover controller that
promotes the standby. Minimum viable version here: standby loads `metadata.json`
read-only, a leader-lease (e.g. via a small coordination file or ZooKeeper) decides
who answers on port 5000.

**Q: How would you generalize placement to N nodes with rack awareness?**
Maintain a rack map (topology.xml equivalent); for chunk i: prefer a node in the
client's rack for replica 1, a different rack for replica 2, a different node in a
third rack for replica 3 (or closest available), balancing by free disk space;
exclude nodes already chosen for this chunk. Real HDFS additionally lets the
*client* propose locations and the NameNode finalize/override.

**Q: What's a block report and why does the NameNode have a handler for it?**
A DataNode's full list of chunks physically on disk. It reconciles "what metadata
claims" with "what's actually stored" — the basis for re-replication, invalidation
and capacity accounting in real HDFS (full report every ~6 h + on startup/on demand
here). Our healer's `chunk_locations` input is *supposed* to come from these;
implementing the DataNode-side sender is the single highest-value next step.

**Q: Name the biggest bottlenecks.**
(1) Single global lock serializes the control plane. (2) Metadata in RAM + JSON
rewritten whole on each mutation (O(files×chunks) I/O per change). (3) Client is the
data path bottleneck — it transmits every replica's worth of bytes (real HDFS's
pipeline spreads that across DataNodes). (4) 2 MB chunks ⇒ more RPC round-trips per
GB than 128 MB blocks (the reason HDFS uses big blocks: amortize RPC/control cost).
(5) No read failover, no retries.

**Q: Why does the client send `num_chunks` but not the data, to the NameNode?**
Placement is a metadata decision: the master knows liveness, replication and policy;
the client knows the bytes. Exchanging only a small descriptor (filename, count,
sizes) lets the master answer with a plan while all bandwidth stays on the
client↔DataNode path — exactly the HDFS design ("the NameNode is the brain, not the
spine").

**Q: How do you verify a download is correct?**
Today: byte order + connection-close. To strengthen: compare total bytes received
with the sum of per-chunk `size` from the NameNode, carry chunk MD5s (already
computed in `split_file`) into the `GET` header so the DataNode's
verify-on-GET path engages, and finally hash the reconstructed file against the
original.

**Q: What would you do differently if this were production?**
Async I/O (asyncio/Netty-style) instead of threads; RPC framework + typed schemas;
edit-log + checkpoints for metadata; block reports + DN-to-DN replication for true
recovery; read failover + checksum on read path; placement with rack awareness and
capacity balancing; authentication (mTLS/Kerberos) — note the UDP heartbeat and
`0.0.0.0` binds are open in the current lab build; HA; metrics (Prometheus) instead
of log-scraping; directories/permissions in the namespace; larger block size +
pipeline writes.