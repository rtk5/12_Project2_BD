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
#### 🖥️ Client (Node 1)
- Uploads and downloads files via the web dashboard.  
- Visualizes chunk distribution and system health.  
- Interacts with the Namenode through sockets or REST API.

#### 🗂️ Namenode (Node 2)
- Splits incoming files into 2MB chunks.  
- Assigns chunks and replicas to Datanodes.  
- Tracks metadata and monitors heartbeats from all Datanodes.  
- Detects failures and triggers re-replication when needed.

#### 💾 Datanodes (Node 3 & Node 4)
- Store and serve file chunks and replicas.  
- Send periodic heartbeats to the Namenode.  
- Maintain data integrity through checksum verification.  
- Handle retrieval requests for file reconstruction.

---

## ⚙️ Core Features

| Feature | Description |
|----------|-------------|
| **Chunking** | Files are split into 2MB blocks before storage. |
| **Replication** | Each chunk is duplicated to ensure fault tolerance. |
| **Heartbeat Monitoring** | Namenode checks the liveness of Datanodes periodically. |
| **File Reconstruction** | Chunks are reassembled to retrieve the original file. |
| **Web Dashboard** | Real-time visualization of chunks, nodes, and replication. |
| **Data Integrity** | Prevents corruption and ensures reliable transfers. |
| **Fault Recovery** | Auto re-replication on Datanode failure (bonus). |

---

## 🛠️ Technology Stack

| Component | Technology |
|------------|-------------|
| **Programming Language** | Python |
| **Networking** | TCP Sockets |
| **Concurrency** | Multi-threading |
| **Backend API / Dashboard** | Flask / FastAPI |
| **Frontend** | HTML, CSS, JavaScript |
| **Visualization** | Chart.js / D3.js (optional) |

---

## 🚀 Getting Started

### 1️⃣ Clone the Repository
```bash
git clone https://github.com/<your-username>/mini-hdfs-simulation.git
cd mini-hdfs-simulation
```

### 2️⃣ Run the Nodes in Order

**Terminal 1 — Namenode**
```bash
python namenode.py
```

**Terminal 2 — Datanode #0**
```bash
python datanode_0.py
```

**Terminal 3 — Datanode #1**
```bash
python datanode_1.py
```

**Terminal 4 — Client + Dashboard**
```bash
python client.py
```

Then, open your browser and navigate to:
```
http://localhost:5000
```

---

## 🧮 Project  Breakdown

### Node Responsibilities 

| Node | Key Components |
|------|----------------|
| **Client UI** | Upload/Download modules, API integration, visualization, UI design |
| **Namenode** | Chunking, metadata, replication, heartbeat management, recovery |
| **Datanode #0** | Chunk storage, heartbeat sender, retrieval logic, error handling |
| **Datanode #1** | Replica handling, consistency checks, recovery robustness |




## 🧾 Example Workflow

1. **User Uploads File** → Client sends file to Namenode.
2. **Chunking** → Namenode splits file into 2MB parts.
3. **Replication Assignment** → Each chunk stored on two different Datanodes.
4. **Heartbeat Monitoring** → Namenode ensures Datanodes are alive.
5. **Download Request** → Client requests reconstruction; Datanodes send chunks.
6. **Reassembly** → Client merges chunks to form the original file.

---

## 📊 Dashboard Preview
*(Example — to be replaced with screenshots once implemented)*

| Component | Description |
|-----------|-------------|
| **Chunk Map** | Shows where each chunk is stored (Node 0 / Node 1). |
| **Node Health** | Displays live heartbeat and node activity. |
| **File Status** | Tracks uploads, replication status, and retrieval success. |

---

## 🧱 Future Enhancements

- Add secondary Namenode checkpointing.
- Integrate checksum validation for corruption detection.
- Use WebSockets for live updates on dashboard.
- Implement Datanode recovery and auto-replication logic.

---


## 🏁 Conclusion

The **Mini HDFS Simulation** is a hands-on exploration of distributed systems fundamentals, combining networking, threading, and data reliability — all visualized through an interactive dashboard.

This project mirrors the core design principles of real-world HDFS, providing a strong foundation for understanding scalable data infrastructures.

---

⭐ **If you found this project useful, consider giving it a star!**
