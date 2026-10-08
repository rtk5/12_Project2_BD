# 🕸️ Mini HDFS Simulation — Exploring Distributed Data Storage

### 📘 Course: Big Data 2025 (UE23CS343AB2)
**Team Members:**  
- Rithvik Matta (PES2UG23CS485)
- Retesh G S (PES2UG23CS475)
- Rishil Abhijit Jalsagi (PES2UG23CS482)
- Rithvik Hemanth (PES2UG23CS484)

---

## 📖 Overview
Modern big data systems rely on robust **distributed file systems** to ensure reliability, scalability, and fault tolerance.  
This project simulates a miniature version of **HDFS (Hadoop Distributed File System)** — the foundational layer behind many large-scale data platforms.

Our **Mini HDFS Simulation** recreates the essence of distributed storage:
- Files are split into fixed-size chunks (2MB).
- Chunks are replicated across multiple Datanodes.
- The Namenode manages metadata, replication, and node health via heartbeats.
- A user-friendly **Flask-based web dashboard** allows file uploads, downloads, and chunk visualization.

### 📄 Deep-dive documentation

| Document | What it covers |
|----------|----------------|
| [explanation.md](explanation.md) | Full project explanation: architecture, every wire protocol, the complete upload/download/heartbeat/healing pipelines step-by-step, and a detailed **comparison with real HDFS**. |
| [detils.md](detils.md) | Function-by-function **code walkthrough** of every file, concept checklist, and interview Q&A. |

---

## 🎯 Objectives
- Design and implement a **distributed file storage simulation**.  
- Support **fault-tolerant chunk storage** using replication.  
- Enable **Namenode–Datanode communication** with periodic heartbeats.  
- Build an **interactive dashboard** to visualize file distribution, node health, and chunk mapping.  
- Guarantee **data consistency and zero corruption** during upload/download operations.

---

## 🧩 System Architecture

```
+-------------------+         +-----------------+
|                   |         |                 |
|   Client (UI)     | <-----> |    Namenode     |
| (Upload/Download) |         |  (Controller)   |
|                   |         |                 |
+--------+----------+         +--------+--------+
         |                             |
         |                             |
         v                             v
+-------------------+         +-------------------+
|   Datanode #0     |         |   Datanode #1     |
| (Storage Node A)  |         | (Storage Node B)  |
+-------------------+         +-------------------+
```

### 🧠 Node Roles
#### 🖥️ Client (`Client/client.py`)
- Uploads and downloads files via the web dashboard.  
- Splits files into 2MB chunks and computes MD5 checksums.  
- Streams chunk data **directly to Datanodes** (never through the Namenode).  
- Interacts with the Namenode through TCP + framed JSON.

#### 🗂️ Namenode (`Namenode/namenode.py`)
- Splits incoming uploads into a **chunk placement plan**.  
- Assigns chunks and replicas to Datanodes (alternating even/odd primary).  
- Tracks metadata (persisted in `metadata.json`) and monitors heartbeats.  
- Detects failures (heartbeat timeout) and runs a replication healer.

#### 💾 Datanodes (`DATANODE0/datanode0.py`, `Datanode1/datanode1.py`)
- Store and serve file chunks and replicas.  
- Send periodic UDP heartbeats to the Namenode every 3 seconds.  
- Maintain data integrity through MD5 checksum verification.  
- Handle retrieval requests for file reconstruction.

---

## ⚙️ Core Features

| Feature | Description |
|----------|-------------|
| **Chunking** | Files are split into 2MB blocks before storage. |
| **Replication** | Each chunk is duplicated (factor 2) to ensure fault tolerance. |
| **Heartbeat Monitoring** | Namenode checks the liveness of Datanodes (UDP pings, 10s timeout). |
| **File Reconstruction** | Chunks are reassembled in order to retrieve the original file. |
| **Web Dashboard** | Real-time upload, download, live logs and system status (Flask). |
| **Data Integrity** | MD5 checksums detect corruption on store and on retrieval. |
| **Fault Recovery** | Replication healer keeps the replica count toward the target (bonus). |

---

## 🛠️ Technology Stack

| Component | Technology |
|------------|-------------|
| **Programming Language** | Python 3.8+ |
| **Networking** | Raw TCP sockets (framed JSON + binary) and UDP heartbeats |
| **Concurrency** | Multi-threading (thread-per-connection, daemon worker threads) |
| **Backend API / Dashboard** | Flask |
| **Frontend** | HTML, CSS, JavaScript, Bootstrap 5 |
| **Metadata Store** | In-memory dict + `metadata.json` (JSON persistence) |
| **Integrity** | MD5 checksums |

---

## 📁 Project Structure

```
12_Project2_BD/
├── README.md
├── requirements.txt          # only third-party dependency: Flask
├── explanation.md            # architecture + pipeline + real-HDFS comparison
├── detils.md                 # code walkthrough + interview Q&A
├── config.json               # reference copy of the cluster config
├── Namenode/
│   ├── namenode.py           # master: metadata, placement, heartbeats, healer
│   ├── config.json           # copy used by the Namenode
│   └── metadata.json         # persisted namespace (file -> chunks -> replicas)
├── DATANODE0/
│   ├── datanode0.py          # storage node A
│   ├── config.json
│   └── storage_dn0/          # created at runtime — chunk files land here
├── Datanode1/
│   ├── datanode1.py          # storage node B
│   ├── config.json
│   └── storage_dn1/          # created at runtime — replica files land here
└── Client/
    ├── client.py             # chunking, transfer, Flask dashboard (port 8080)
    └── config.json
```

---

## 🚀 Installation & Running

### 1️⃣ Prerequisites
- **Python 3.8 or newer**
- `pip` (bundled with Python; on some systems `sudo apt install python3-pip`)

### 2️⃣ Clone the Repository
```bash
git clone <repo-url>
cd 12_Project2_BD
```

### 3️⃣ (Recommended) Create a Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
```

### 4️⃣ Install Dependencies
```bash
pip install -r requirements.txt
```
> Only **Flask** is required. Everything else (sockets, threading, JSON, struct,
> hashlib, logging) is part of the Python standard library.

### 5️⃣ Configure the Cluster (`config.json`)
Each component reads **its own** `config.json` from its folder
(`Namenode/config.json`, `DATANODE0/config.json`, `Datanode1/config.json`,
`Client/config.json`). The committed values target a 4-VM lab topology, so:

- **Multi-machine (VM lab):** set each component's host/IP to match your VMs —
  `namenode.host`, and the two `datanodes.dn0/dn1` entries.
- **Single machine (one box):** set **every** host to `"localhost"` in all four
  config files.

**Port map (defaults in the config):**

| Port | Protocol | Used by | Purpose |
|------|----------|---------|---------|
| 5000 | TCP | Namenode ←→ Client | Control plane (upload plan, commit, download locations) |
| 5001 | UDP | Datanodes → Namenode | Heartbeats |
| 6001 | TCP | Datanode 0 | Chunk `STORE` / `GET` |
| 6002 | TCP | Datanode 1 | Chunk `STORE` / `GET` |
| 8080 | HTTP | Browser ←→ Client | Flask dashboard |

Make sure none of these ports are already in use (see Troubleshooting).

### 6️⃣ Run the Nodes in Order

**Terminal 1 — Namenode**
```bash
python Namenode/namenode.py
```

**Terminal 2 — Datanode #0**
```bash
python DATANODE0/datanode0.py
```

**Terminal 3 — Datanode #1**
```bash
python Datanode1/datanode1.py
```

**Terminal 4 — Client + Dashboard**
```bash
python Client/client.py
```

Then open your browser and navigate to:
```
http://localhost:8080
```
(Running on different machines? Use the **client machine's IP**:
`http://<client-ip>:8080` — the dashboard binds to `0.0.0.0`.)

You'll see a log line in each terminal confirming startup, e.g.
`[NAMENODE STARTED]`, `[LISTEN] Datanode1 ready on ...`, and the
`[HEARTBEAT]`/`[ALIVE]` traffic between the nodes.

---

## 🧪 Using the Dashboard

1. **Upload** — pick any file and press *Upload*. The dashboard logs show the
   chunking, the placement plan from the Namenode, and each chunk being streamed
   to both Datanodes. Watch the `[STORE_OK]` lines in the Datanode terminals and
   the new files appear in `storage_dn0/` and `storage_dn1/`.
2. **Download** — type the uploaded filename and press *Download*. The client asks
   the Namenode for chunk locations, pulls each chunk in order from the Datanodes
   and writes `reconstructed_<filename>` in `Client/`.
3. **Live logs** — the bottom pane refreshes every 3 seconds with recent client
   activity (upload/download progress, errors).
4. **Verify** — compare the reconstructed file with the original
   (`md5sum original reconstructed_original` or any checksum tool).
5. **Fault test** — kill a Datanode (`Ctrl+C` in its terminal) and watch the
   Namenode log `[DOWN] <dn>` after the 10-second heartbeat timeout; uploads then
   place only on the surviving node.

---

## 🔌 Protocol Quick Reference

- **Client ↔ Namenode (TCP 5000):** framed JSON — `[4-byte big-endian length][JSON]`.
  Actions: `upload_request`, `commit_upload`, `download_request`, `list_files`,
  `block_report`.
- **Client ↔ Datanode (TCP 6001/6002):** `STORE\n` / `GET\n` / `REPLICATE\n`, then
  `[4-byte length][JSON header {chunk_name, size, checksum?}]`, then raw bytes.
  Handshake reply: `READY`; errors: `ERROR:NOT_FOUND`, `ERROR:INTEGRITY_FAIL`,
  `ERROR:CHECKSUM_FAIL`.
- **Datanode → Namenode (UDP 5001):** plain node ID (`dn0` / `dn1`) every 3 s.

Full walkthrough in [explanation.md → §4 Wire protocols](explanation.md).

---

## 🧮 Project Breakdown

### Node Responsibilities

| Node | Key Components |
|------|----------------|
| **Client UI** | Upload/Download modules, chunking + checksums, API integration, visualization, UI design |
| **Namenode** | Chunking plan, metadata persistence, replication, heartbeat management, recovery |
| **Datanode #0** | Chunk storage, heartbeat sender, retrieval logic, error handling |
| **Datanode #1** | Replica handling, integrity verification (store + get), recovery robustness |

---

## 🧾 Example Workflow

1. **User Uploads File** → Client saves it and splits it into 2MB chunks locally.
2. **Placement Plan** → Client asks the Namenode, which replies with per-chunk DataNode addresses.
3. **Replicated Storage** → Client streams each chunk to *both* Datanodes (direct, bypassing the master).
4. **Commit** → Client confirms completion; Namenode persists metadata.
5. **Heartbeat Monitoring** → Datanodes ping the Namenode every 3 s; stale nodes (>10 s) are excluded from plans.
6. **Download Request** → Client requests reconstruction metadata; Namenode returns live replica locations.
7. **Reassembly** → Client fetches chunks in order and writes the original file.

---

## 📊 Dashboard Preview
*(Add screenshots here once captured: upload in progress, chunk map / logs, download result.)*

| Component | Description |
|-----------|-------------|
| **Upload / Download** | File picker + filename box; jobs run in background threads. |
| **Live Log Pane** | Last 100 client log lines, refreshed every 3 s. |
| **Status Pane** | Node health + file distribution (served from the Namenode). |

---

## 🛠️ Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| `Address already in use` | Port 5000/5001/6001/6002/8080 is taken — change it in the relevant `config.json` or stop the other process. The servers set `SO_REUSEADDR`, so quick restarts of the *same* server are fine. |
| Client logs `Error communicating with Namenode` | Namenode not started yet, or wrong `namenode.host`/`client_port` in `Client/config.json`. Start the Namenode **first**. |
| Upload plan says `No live datanodes` | No heartbeat received within the timeout — check Datanodes are running and point at the right `namenode.host` + `heartbeat_port` (UDP!). |
| `[DOWN] dnX missed heartbeats` | That Datanode stopped (or its config points at the wrong Namenode port/host). |
| Download produces a partial file | The first listed replica for some chunk wasn't reachable — check that Datanode; read failover to the second replica is a known extension. |
| Dashboard loads but uploads never proceed | Check the client terminal logs — usually a config mismatch between the client and the Namenode/Datanodes. |
| Files not appearing in `storage_dnX/` | The Datanode's `storage_dir` is **relative to the folder you launched it from** — start each program from its own directory (or make the path absolute in config). |

---

## 🧱 Future Enhancements

- Add secondary Namenode checkpointing / Active-Standby HA.
- Wire up **block reports** from Datanodes (the Namenode handler already exists) so the healer works on ground truth.
- DataNode-to-DataNode re-replication using the existing `REPLICATE` command.
- Read failover across replicas with size/checksum verification on download.
- Implement the `system_status` action on the Namenode to fill the dashboard status pane.
- Use WebSockets for live updates on the dashboard (instead of 3 s polling).
- Generalize placement from 2 hard-coded nodes to an N-node, rack-aware policy.

---

## 🏁 Conclusion

The **Mini HDFS Simulation** is a hands-on exploration of distributed systems fundamentals, combining networking, threading, and data reliability — all visualized through an interactive dashboard.

This project mirrors the core design principles of real-world HDFS, providing a strong foundation for understanding scalable data infrastructures. See [explanation.md](explanation.md) for the side-by-side comparison with real HDFS.

---

⭐ **If you found this project useful, consider giving it a star!**
