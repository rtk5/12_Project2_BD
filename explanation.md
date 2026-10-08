# 📚 Mini HDFS — Complete Project Explanation

> **Course:** Big Data 2025 (UE23CS343AB2)
> **Team:** Rithvik Matta (PES2UG23CS485), Retesh G S (PES2UG23CS475), Rishil Abhijit Jalsagi (PES2UG23CS482), Rithvik Hemanth (PES2UG23CS484)

This document explains the **entire project**: what it is, how it is built, how every
piece of data moves through the system (the full pipeline), and — most importantly for
Big Data interviews — **how it compares to real HDFS** and where we deliberately simplified.

A second document, [`detils.md`](detils.md), goes function-by-function through the actual code.

---

## 1. What is this project?

This project is a **working simulation of HDFS (Hadoop Distributed File System)** built
from scratch in Python. It demonstrates the two fundamental ideas of every large-scale
distributed file system:

1. **Separation of metadata and data.**
   A master node (NameNode) knows *where* every piece of data lives, but it never stores
   the data itself. Worker nodes (DataNodes) store the bytes.

2. **Reliability through replication.**
   Every piece of data is stored on more than one machine, so a single machine failure
   cannot lose data.

A file the user uploads is:

- split into fixed-size **chunks** (2 MB in this project),
- each chunk is **replicated** onto the DataNodes (replication factor = 2),
- the **NameNode** keeps metadata (file → chunks → DataNode addresses),
- DataNodes stay **alive-checked** via periodic heartbeats,
- a **Flask dashboard** lets the user upload files, download them back, and watch
  live logs/status.

The upload and download paths **bypass the NameNode for the actual bytes** — the
NameNode only issues "placement instructions" (addresses), and the client streams data
directly to the DataNodes. This is the same core idea as real HDFS.

---

## 2. Components and their responsibilities

| Component | File | Role |
|-----------|------|------|
| **NameNode** | `Namenode/namenode.py` | Master/coordination. Stores metadata in memory + `metadata.json`. Splits upload into a chunk *placement plan*, answers download requests with chunk locations, listens for UDP heartbeats, monitors node liveness, and tries to keep the replication factor satisfied. **Never touches file content.** |
| **DataNode 0** | `DATANODE0/datanode0.py` | Storage node. TCP server that stores chunks it receives (`STORE`) and serves them on request (`GET`). Sends a UDP heartbeat every 3 s. Computes MD5 checksums of stored chunks. |
| **DataNode 1** | `Datanode1/datanode1.py` | Storage node (the "replica" node). Same job as DN0, plus a slightly stricter write path (checksum verification on store, `REPLICATE` command support). |
| **Client** | `Client/client.py` | User side. Splits files into chunks, asks the NameNode for a placement plan, pushes each chunk to the assigned DataNodes, reconstructs files on download, and runs a **Flask web dashboard** (upload / download / logs / status). |
| **Config** | `config.json` (one copy per component dir) | Hosts, ports, chunk size, replication factor, heartbeat interval/timeout. |
| **Metadata** | `Namenode/metadata.json` | The persisted NameNode namespace (files → chunks → replica locations). |

### Node roles in plain words

- **Client** — "I have a file. Tell me where to put its pieces."
- **NameNode** — "Put piece 0 on dn0, piece 1 on dn1, each with a copy on the other."
- **DataNodes** — "Here is my chunk. / Give you my chunk. / *ping* I'm alive."

---

## 3. Architecture

```
                          +----------------------------------+
                          |            CLIENT                |
                          |  (Flask dashboard, port 8080)    |
                          |  split / chunk / reassemble      |
                          +---------+--------------+---------+
                                    |              ^
                    TCP, JSON       |              |  TCP, JSON
            (upload/download plan)  |              |  (chunk locations)
                                    v              |
                          +----------------------------------+
                          |            NAMENODE              |
                          |  TCP 5000 (control)   UDP 5001   |
                          |  metadata, placement,            |
                          |  heartbeat monitor, healer       |
                          +---------+--------------+---------+
                                    |              ^
                    UDP "dn0"       |              |  UDP "dn1"
                    heartbeats      |              |  heartbeats
                                    v              |
                          +----------------+       |
        TCP 6001          |   DATANODE 0   |  +----+--------+
   (STORE / GET chunks)-->|  storage_dn0/  |  |    ...       |
                          +----------------+  +--------------+
                          +----------------+
        TCP 6002          |   DATANODE 1   |
   (STORE / GET chunks)-->|  storage_dn1/  |
                          +----------------+
```

### Port map (from `config.json`)

| Port | Protocol | Used by | Purpose |
|------|----------|---------|---------|
| **5000** | TCP | NameNode ←→ Client | Control plane: upload plan, commit, download locations, file list, block reports |
| **5001** | UDP | DataNodes → NameNode | Heartbeats (small node-ID messages) |
| **6001** | TCP | DataNode 0 ←→ Client | Chunk data: `STORE` / `GET` |
| **6002** | TCP | DataNode 1 ←→ Client | Chunk data: `STORE` / `GET` |
| **8080** | HTTP | Browser ←→ Client | Flask dashboard |

**Design point (interview gold):** the system has a *control plane* (small JSON
messages over TCP + UDP pings) and a *data plane* (raw bytes over TCP). All heavy
traffic flows directly between client and DataNodes — the NameNode is never the
bottleneck for bandwidth, exactly like HDFS.

### Threading model

| Process | Threads |
|---------|---------|
| NameNode | 1 per client connection + heartbeat listener + heartbeat monitor + replication healer |
| DataNode 0 | heartbeat sender (daemon) + chunk TCP server (main thread) |
| DataNode 1 | heartbeat sender (daemon) + chunk TCP server (main thread) |
| Client | Flask (internal worker threads) + one thread per upload/download job |

The NameNode is the only process with **shared mutable state** accessed by many
threads, so it protects `metadata`, `chunk_locations` and `heartbeat_table` with a
single `threading.Lock` (`state_lock`).

---

## 4. Wire protocols (what actually travels over the network)

Three different protocols are used:

### 4.1 NameNode control protocol (TCP)

TCP is a byte stream — it does **not** preserve message boundaries — so every message
is **framed**:

```
+----------------------+------------------------+
| 4-byte length (BE)   | JSON payload (UTF-8)   |
+----------------------+------------------------+
```

- The 4-byte length is packed big-endian (`struct.pack(">I", n)`) — network byte
  order, so any CPU architecture agrees on the number.
- A 128 MB `MAX_JSON_FRAME` cap protects against malformed/malicious length fields.
- `recv_exact()` loops on `recv()` until exactly *n* bytes arrive, because a single
  `recv()` may return fewer bytes than requested.

**Messages (JSON "action" field):**

| Action | From | To | Payload | Response |
|--------|------|----|---------|----------|
| `upload_request` | Client | NN | `filename`, `num_chunks` | `{status: ok, plan: [per-chunk DataNode lists]}` |
| `commit_upload` | Client | NN | `filename` | `{status: ok, message: "commit recorded"}` |
| `download_request` | Client | NN | `filename` | `{status: ok, metadata: [per-chunk live DataNode lists]}` |
| `list_files` | Client | NN | — | `{status: ok, files: [...]}` |
| `block_report` | (DataNode)* | NN | `dn_id`, `blocks: [{filename, chunk_id}]` | `{status: ok}` |

Each request uses a **one-shot connection**: connect → send one JSON → receive one
JSON → close.

### 4.2 DataNode chunk protocol (TCP)

```
 1. command line:   "STORE\n"  |  "GET\n"  |  "REPLICATE\n"
 2. header length:  4-byte big-endian integer
 3. JSON header:    {"chunk_name": "a.txt.chunk0", "size": 2097152 [, "checksum": "..."]}
 4. (STORE)         DataNode replies 5 bytes: "READY"
 5. (STORE)         raw chunk bytes  (exactly `size` bytes)
 6. (GET)           raw chunk bytes until the DataNode closes the connection
```

Error replies from a DataNode: `ERROR:NOT_FOUND`, `ERROR:INTEGRITY_FAIL`,
`ERROR:CHECKSUM_FAIL`.

### 4.3 Heartbeat protocol (UDP)

Every 3 seconds each DataNode opens a UDP socket and sends its own ID — literally the
string `"dn0"` or `"dn1"` — to `namenode_host:5001`. UDP because heartbeats are tiny,
frequent, and lossy-tolerant: if one ping is dropped, the next one arrives 3 s later;
we never want a heartbeat to block on retransmission.

The NameNode marks a DataNode dead when
`now − last_heartbeat > heartbeat_timeout_sec` (10 s in the config).

---

## 5. The full pipeline, step by step

### 5.1 UPLOAD pipeline

```
Browser          Flask(8080)        Client thread           NameNode(5000)      DataNode0(6001)   DataNode1(6002)
   |  file pick     |                    |                        |                     |                   |
   |----------------> POST /upload       |                        |                     |                   |
   |                  |  save to ./file  |                        |                     |                   |
   |                  |-----------------> upload_file()           |                     |                   |
   |                  |  202 "Uploading" |                        |                     |                   |
   |<-----------------|                  |                        |                     |                   |
   |                  |        1) split_file(): read 2 MB at a time, MD5 each chunk     |                   |
   |                  |        2) TCP connect -> NameNode         |                     |                   |
   |                  |        {"action":"upload_request",        |                     |                   |
   |                  |         "filename":"a.txt",               |                     |                   |
   |                  |         "num_chunks":3} ------------------>                     |                   |
   |                  |                  |            NameNode: takes state_lock       |                   |
   |                  |                  |            live = live_datanodes_ids()      |                   |
   |                  |                  |            plan_two_replicas() per chunk:   |                   |
   |                  |                  |              chunk0 -> dn0 (+dn1)           |                   |
   |                  |                  |              chunk1 -> dn1 (+dn0)           |                   |
   |                  |                  |              chunk2 -> dn0 (+dn1)           |                   |
   |                  |                  |            metadata["a.txt"] = [chunks...]  |                   |
   |                  |                  |            save metadata.json               |                   |
   |                  |                  |  <--- {status:ok, plan:[...]}                |                   |
   |                  |        3) for EACH chunk, for EACH assigned DataNode:          |                   |
   |                  |          TCP connect -> DataNode                                   |                   |
   |                  |          "STORE\n" --------------------------------------------->                   |
   |                  |          [4B len][{chunk_name, size}] ------------------------->                   |
   |                  |          <-------------------------- "READY"                        |                   |
   |                  |          raw bytes (2 MB) ------------------------------------->                   |
   |                  |          (same chunk also streamed to the replica DataNode)                          |
   |                  |        4) commit: TCP -> NameNode                                          |
   |                  |          {"action":"commit_upload","filename":"a.txt"} ------------->               |
   |                  |          <--- {status:ok, message:"commit recorded"}                                |
   |                  |        5) log "Upload complete and committed"                                   |
```

**What each step does internally:**

1. **Chunking** — `split_file()` opens the file in binary mode and reads it in
   `CHUNK_SIZE` (2 MB) reads until EOF. The last chunk is usually smaller. An MD5
   digest is computed for every chunk.
2. **Placement planning (NameNode)** — the client sends *only the filename and the
   chunk count* — never data. Under the lock, the NameNode:
   - finds which DataNodes are alive (`live_datanodes_ids()`),
   - runs `plan_two_replicas(cid, live)` for every chunk id: with both nodes alive,
     **even chunk ids** make dn0 the *primary* and dn1 the *secondary*, odd ids swap
     (a simple alternating load distribution). Each chunk is assigned to **both**
     live nodes (replication factor 2). If only one node is alive, the chunk goes
     there alone; if none, the upload is refused.
   - writes the new entry into `metadata[filename]` and **persists `metadata.json`**.
   - returns the plan.
3. **Data transfer (Client → DataNodes, direct)** — for each chunk, for each address
   in the plan, `send_chunk()` opens a new TCP connection to that DataNode, sends the
   `STORE` command + JSON header, waits for the `READY` handshake, then streams the
   raw bytes. The DataNode reads exactly `size` bytes, computes its MD5, and writes
   `storage_dnX/<filename>.chunkN` to disk.
4. **Commit** — the client tells the NameNode the transfer finished; the NameNode
   confirms the file is in its namespace and re-persists metadata.
5. **Result** — every chunk now exists on both DataNodes; `metadata.json` maps the
   file to all replica addresses.

### 5.2 DOWNLOAD pipeline

```
Browser          Flask(8080)        Client thread           NameNode(5000)      DataNodes
   |  filename       |                    |                        |              |
   |----------------> POST /download      |                        |              |
   |                  |-----------------> download_file()          |              |
   |                  |  1) {"action":"download_request",          |              |
   |                  |     "filename":"a.txt"} ----------------->               |
   |                  |        NameNode: for each chunk keep the  |              |
   |                  |        REPLICA ADDRESSES THAT ARE ALIVE   |              |
   |                  |  <--- {status:ok, metadata:[{chunk_id,     |              |
   |                  |         chunk_name, datanodes:[...], size}]}             |
   |                  |  2) for EACH chunk in order:              |              |
   |                  |     TCP connect -> first listed DataNode              |
   |                  |     "GET\n" ------------------------------------------->
   |                  |     [4B len][{chunk_name}] -------------------------------->
   |                  |     <---------------------- raw chunk bytes (until FIN)  |
   |                  |     append bytes to reconstructed_a.txt                     |
   |                  |  3) log "File reconstructed as reconstructed_a.txt"        |
   |                  |  200 "Downloading ..." (already sent, work was backgrounded)
```

The reconstruction is correct **because chunks are appended to the output file in
chunk-id order** — the file is simply the concatenation of its chunks.

### 5.3 Heartbeat & liveness pipeline

```
every 3 s:   dn0 --UDP--> NN:5001  payload "dn0"     (heartbeat_listener thread)
             dn1 --UDP--> NN:5001  payload "dn1"                 heartbeat_table[dn] = time.time()

every 5 s:   heartbeat_monitor thread:
                for each dn: elapsed = now - heartbeat_table[dn]
                             elapsed > 10 s  ->  log "[DOWN] dnX missed heartbeats"
                             else            ->  log "[ALIVE] dnX"

on demand:   live_datanodes_ids() recomputes liveness from heartbeat_table
             (used by upload planning and download location filtering)
```

Liveness is **derived state** — nothing is ever "marked dead"; a node is simply
excluded from the live set when its last heartbeat is older than the timeout. A
node that restarts and sends fresh pings re-enters the live set automatically.

### 5.4 Block reports (designed, partially wired)

The NameNode understands a `block_report` message: a DataNode connects over TCP to
port 5000 and says *"these are the chunks I physically hold"*. The NameNode merges
that list into `chunk_locations[(filename, chunk_id)] = {dn_ids...}`.

**Why this matters:** heartbeats only tell the NameNode that a node is *up*; block
reports tell it *what is actually on disk*. Real HDFS uses block reports as the
ground truth for replicas. In this codebase the NameNode side is implemented, but no
component currently *sends* block reports — so `chunk_locations` stays empty (see
§8, Limitations).

### 5.5 Replication healing pipeline

A background thread (`replication_healer`) runs every 10 s:

1. Compute the set of live DataNodes.
2. For every file, every chunk: look up which nodes *hold* it (`chunk_locations`).
3. If `holders < replication_factor`, pick live DataNodes that don't already hold
   the chunk and add them to `rec["replicas"]` in the metadata; persist
   `metadata.json`. Log `[HEAL] file chunkN → added dnX`.

**Intended behaviour:** when a node dies and later the system has enough nodes,
under-replicated chunks get new replica assignments so the replication factor is
restored. In the current wiring the holder set is empty (no block reports), so the
healer treats every chunk as under-replicated — this is the single most important
gap to close before the recovery story is complete (see §8).

### 5.6 Dashboard pipeline (Client → Browser)

- `GET /` → Flask serves an inline HTML page (Bootstrap from CDN): an upload form,
  a download form, and a terminal-style log pane.
- `POST /upload` → file saved to the client's working dir, **upload runs in a
  background thread** so the HTTP response returns immediately.
- `POST /download` → same, background thread writes `reconstructed_<name>`.
- `GET /logs` → last 100 lines captured in-memory by a custom
  `logging.Handler` (`LogCaptureHandler` keeps a rolling buffer of 200 records).
- `GET /status` → asks the NameNode for `system_status` and formats node health,
  file distribution and integrity into text (the NameNode action is not yet
  implemented, so today this section is empty — see §8).
- The browser **polls `/logs` + `/status` every 3 s** with `fetch()`, so the page
  shows near-live activity without any server push.

---

## 6. State & data layout

### In-memory state (NameNode)

```python
metadata          = { "a.txt": [ {"chunk_id": 0,
                                  "chunk_name": "a.txt.chunk0",
                                  "replicas": [["192.168.191.47", 6001],
                                               ["192.168.191.86", 6002]],
                                  "size": 2097152}, ... ] }
chunk_locations   = { ("a.txt", 0): {"dn0", "dn1"} }        # fed by block reports
heartbeat_table   = { "dn0": 1728300003.4, "dn1": 1728300005.1 }
```

### On-disk layout

```
Namenode/
  namenode.py
  config.json
  metadata.json          <- persisted namespace (file -> chunks -> replica hosts)
DATANODE0/
  datanode0.py  config.json
  storage_dn0/a.txt.chunk0   <- actual chunk bytes
  storage_dn0/a.txt.chunk1
Datanode1/
  datanode1.py  config.json
  storage_dn1/a.txt.chunk0   <- replica bytes
Client/
  client.py  config.json
  a.txt                      <- uploaded file saved by Flask
  reconstructed_a.txt        <- downloaded file written by client
```

### Replication factor

`replication_factor: 2` — every chunk is streamed to two DataNodes at upload time.
One DataNode failure therefore never loses data: the other replica keeps every
chunk, and the NameNode's download handler only returns *live* replica addresses.

---

## 7. Comparison with real HDFS

This is the section interviewers will dig into. The honest framing:
**"We reproduced the *architecture and control logic* of HDFS at teaching scale;
these are the places where real HDFS goes further."**

| Aspect | Real HDFS | This mini-HDFS |
|--------|-----------|----------------|
| **Block size** | 128 MB default (HDFS-3), historically 64 MB; tunable per filesystem | 2 MB fixed via `chunk_size_mb` (small so the effect is visible in a lab) |
| **Replication factor** | Default 3, configurable per file (`-replication`) | 2, global in config |
| **Block placement** | Smart, **rack-aware** policy: replica 1 on the writer's rack, replica 2 on a different rack, replica 3 on a different node; NameNode *suggests* locations, DataNodes confirm | Alternating even/odd chunk-id assignment between dn0/dn1 (primary swaps), both nodes get a copy |
| **Write path** | Client opens an `FSDataOutputStream`; NameNode returns a **DataNode pipeline**; client writes to the *first* node and **DataNodes forward the stream to the next** (pipeline replication, done while streaming) | Client pulls each chunk itself to **every** replica DataNode independently (no DataNode→DataNode streaming) |
| **Read path** | NameNode returns replicas **sorted by proximity** (rack/colocation); client reads from the nearest; on failure it **fails over to the next replica** | NameNode returns live replicas; client reads from the **first** listed address only (no failover yet) |
| **Metadata store** | In-memory INode/BlockManager + **FSImage + EditLog** on disk; periodic checkpoints to secondary NameNode / Checkpoint Node | In-memory dict + `metadata.json` rewritten on upload/commit/heal |
| **NameNode HA** | Active/Standby NameNodes with a shared JournalNode quorum (QJM) or KJM; automatic failover via HealthMonitor/FailoverController | Single NameNode; no standby (metadata.json survives NN restart, but no zero-downtime HA) |
| **Heartbeats** | DataNode → NameNode every **3 s** (liveness + disk usage); **NameNode → DataNode** replies every ~3 s carrying commands (re-replicate, invalidate, report) | One-way UDP pings (DataNode → NameNode) every 3 s, 10 s timeout; **no NameNode→DataNode command channel** |
| **Block reports** | Full report on startup + every ~6 h (default), and on demand; the authoritative source of "who has what" | `block_report` handled by the NameNode, but no component sends one yet — the natural next step |
| **Recovery / re-replication** | NameNode computes under-replicated blocks and **issues replication commands to live DataNodes**, which pull data from each other (data never re-traverses the client) | `replication_healer` thread adjusts the *metadata* toward the target factor; actual DataNode-to-DataNode copying is not implemented |
| **Checksums** | Per **512-byte sub-block**, CRC32 (default), stored alongside the block (`.crc` file); verified on read and on pipeline write; corrupt block → fail over to next replica | MD5 over the whole 2 MB chunk; computed on both ends (DN1 verifies on store when a reference is supplied; both verify on `GET` when a checksum is supplied) — used to *detect* corruption, not per-sub-block |
| **Namespace** | Full tree of Inodes: directories, permissions, ownership, snapshots, quotas | Flat map `filename → [chunks]` (no directories) |
| **Security** | Kerberos auth, delegation tokens, ACLs, proxy-user checks | None (lab environment) |
| **Transport / RPC** | Hadoop RPC (Jetty/Netty, protobuf/Avro serialization), sequence IDs, async RPC | Hand-rolled: framed JSON over TCP + line commands + raw bytes |
| **User interface** | HDFS Web UI (port 50070/9870), `hdfs dfs` CLI, API clients | Flask dashboard: upload, download, live logs, status |
| **Scheduling / admission** | Block placement considers rack topology, disk balancing, `dfs.datanode.*` policies | Two hard-coded nodes; trivial scheduling |

### The five biggest conceptual differences, explained

1. **Pipeline vs. client-fan-out writes.** Real HDFS: client → DN1 → DN2 → DN3 in a
   single streaming pipeline. Here the client sends the same bytes N times. At lab
   scale that's fine; at real scale it multiplies client-side bandwidth and breaks if
   one leg dies mid-stream (no pipeline restart).

2. **Rack-aware placement.** Real HDFS's placement algorithm balances *availability*
   (survive a whole rack failure) against *bandwidth* (reads should be local). Our
   even/odd alternation is the simplest possible load balancer — an honest
   simplification to call out.

3. **Block reports as ground truth.** In real HDFS, "what the metadata says" and
   "what's actually on disk" are reconciled by block reports. Without them our
   healer's input (`chunk_locations`) is empty — the most instructive gap in the
   project and the clearest roadmap item.

4. **Two-way NameNode↔DataNode channel.** Real HDFS heartbeats *carry commands*
   (replicate this block, invalidate that one). We only have liveness pings, so the
   NameNode currently can only fix its *metadata*, not order a physical copy.

5. **HA and checkpointing.** Real HDFS tolerates NameNode failure with Active/Standby
   and a journal quorum. We persist `metadata.json` (survives restarts) but any NN
   downtime halts the control plane.

### What this simulation gets *right* (and worth saying in an interview)

- Metadata/data separation; NameNode never sees file bytes.
- One control message per operation; bulk data flows client↔DataNode directly.
- Framed binary protocol over a byte-stream transport (TCP) — the exact framing
  problem every RPC system solves.
- Heartbeat-based liveness with a timeout, and liveness used to *filter* locations.
- Replication as the fault-tolerance primitive; checksums for integrity detection.
- Metadata durability across master restarts.

---

## 8. Known limitations & roadmap

Be upfront about these — they double as interview talking points.

1. **Block reports are received but never sent.** `chunk_locations` is always empty,
   so the replication healer has no ground truth. *Fix:* DataNode sends its stored
   chunk list on startup and periodically (TCP to port 5000).
2. **No DataNode→DataNode replication.** Healing edits metadata; nothing physically
   copies a chunk between DataNodes. *Fix:* use the already-supported `REPLICATE`
   command on DN1.
3. **No read failover.** Download uses the first listed replica; a dead endpoint
   aborts the file (partial write). *Fix:* loop over `datanodes` per chunk, verify
   size/checksum, try the next on failure.
4. **`system_status` action** is requested by the dashboard but not implemented on
   the NameNode, so the status pane is empty.
5. **Last-chunk size is not communicated** to the NameNode (`chunk_sizes` is optional
   in the protocol but the client doesn't send it), so metadata records the standard
   chunk size for every chunk.
6. **Placement is hard-coded to exactly two nodes** (`plan_two_replicas`), not a
   general N-node policy.
7. **Checksums are computed on upload but not transmitted** in the store header, so
   the DataNode-side store verification path (present in DN1) is never exercised.
8. **No HA**, no directories/permissions, no security, no client retry on the
   control plane (a failed NameNode connection returns `None` and the operation is
   abandoned).
9. **Single-machine demo caveat:** the committed configs point at four separate VM
   IPs (a lab topology). For a one-machine run, set every host to `localhost` in
   all four `config.json` files.

---

## 9. Running the system (quick reference)

Full instructions live in [README.md](README.md). Short version:

```bash
pip install -r requirements.txt        # Flask is the only dependency

# Terminal 1: Namenode
python Namenode/namenode.py
# Terminal 2: DataNode 0
python DATANODE0/datanode0.py
# Terminal 3: DataNode 1
python Datanode1/datanode1.py
# Terminal 4: Client + dashboard
python Client/client.py
# then open http://localhost:8080
```

---

## 10. One-paragraph summary (interview elevator pitch)

> We built a working miniature HDFS in Python: a NameNode that owns only metadata —
> files, chunk lists, replica placement, and DataNode liveness — two DataNodes that
> store 2 MB chunks with MD5 integrity checks, and a client that chunks files,
> negotiates a placement plan over a hand-rolled framed-JSON TCP protocol, streams
> data directly to DataNodes (never through the master), and reconstructs files on
> download through a Flask dashboard. Reliability comes from replication factor 2,
> heartbeat-based liveness that filters live replicas, and a metadata-persisting
> replication healer. Against real HDFS we deliberately simplified block size,
> rack-aware placement, the DataNode pipeline write path, block-report-driven
> recovery, HA, and security — and we can walk through each of those differences and
> how we'd close them.
