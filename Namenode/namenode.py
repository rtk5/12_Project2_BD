#!/usr/bin/env python3

# ============================================================
# NAMENODE - MINI HDFS
# ============================================================
#
# The NameNode is the MASTER / COORDINATION component of our
# mini-HDFS implementation.
#
# IMPORTANT:
# The NameNode DOES NOT store the actual file contents.
#
# It stores metadata such as:
#   1. Which files exist
#   2. How each file is split into chunks
#   3. Which DataNodes contain each chunk
#   4. Whether DataNodes are alive
#   5. Which chunks need additional replicas
#
# DataNodes are responsible for actually storing the data.
#
# Communication:
#   Client <----TCP----> NameNode
#   DataNode ----UDP----> NameNode  (heartbeats)
#
# ============================================================


import socket
import threading
import json
import time
import struct
import os
import logging

from typing import Dict, List, Tuple, Set


# ============================================================
# LOGGING SETUP
# ============================================================
#
# Logging is used instead of simply using print().
#
# It helps us understand what the distributed system is doing:
#
#   [UPLOAD INIT]
#   [HEARTBEAT]
#   [DOWN]
#   [HEAL]
#   [BLOCK REPORT]
#
# The format contains:
#   timestamp
#   log level
#   thread name
#   actual message
#
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)

log = logging.getLogger("namenode")


# ============================================================
# LOAD CONFIGURATION
# ============================================================
#
# Instead of hardcoding ports, DataNodes, replication factor,
# etc., we keep them inside config.json.
#
# Example config:
#
# {
#   "namenode": {
#       "host": "localhost",
#       "client_port": 9000,
#       "heartbeat_port": 9001
#   },
#   "datanodes": {
#       "dn0": {"host": "localhost", "port": 9100},
#       "dn1": {"host": "localhost", "port": 9200}
#   },
#   "replication_factor": 2,
#   "heartbeat_timeout_sec": 15,
#   "chunk_size_mb": 2
# }
#
# ============================================================

def load_config():
    """
    Reads the configuration file and converts the JSON
    contents into a Python dictionary.
    """

    with open("config.json") as f:
        return json.load(f)


# Load configuration once when NameNode starts.
config = load_config()


# ------------------------------------------------------------
# Extract individual configuration values.
# ------------------------------------------------------------

HOST = config["namenode"]["host"]

# TCP port used for communication between clients and NameNode.
CLIENT_PORT = int(config["namenode"]["client_port"])

# UDP port used by DataNodes to send heartbeats.
HEARTBEAT_PORT = int(config["namenode"]["heartbeat_port"])

# Dictionary containing information about all known DataNodes.
#
# Example:
#
# DATANODES = {
#     "dn0": {"host": "localhost", "port": 9002},
#     "dn1": {"host": "localhost", "port": 9003}
# }
#
DATANODES: Dict[str, Dict] = config["datanodes"]


# Desired number of copies of every chunk.
#
# For example:
#
# REPLICATION = 2
#
# means ideally every chunk should exist on 2 DataNodes.
#
REPLICATION = int(config["replication_factor"])


# How long we wait without receiving a heartbeat before
# considering a DataNode dead.
HEARTBEAT_TIMEOUT = float(config["heartbeat_timeout_sec"])


# Chunk size is specified in MB in config.json.
#
# Convert MB -> bytes because file/network operations work
# with bytes.
#
# Example:
#
# 2 MB = 2 * 1024 * 1024 bytes
#
CHUNK_BYTES = config.get("chunk_size_mb", 2) * 1024 * 1024


# ============================================================
# METADATA PERSISTENCE
# ============================================================
#
# The NameNode's most important responsibility is maintaining
# metadata.
#
# If metadata was stored only in RAM, restarting the NameNode
# would cause us to lose information about previously uploaded
# files.
#
# Therefore we persist metadata into:
#
#       metadata.json
#
# This is a simplified version of how a real HDFS NameNode
# persists its namespace.
#
# ============================================================

METADATA_FILE = "metadata.json"


def save_metadata(metadata):
    """
    Persist the current metadata dictionary to disk.
    
    This ensures that file/chunk information survives a
    NameNode restart.
    """

    try:

        # Open metadata.json in write mode.
        with open(METADATA_FILE, "w") as f:

            # Convert Python dictionary -> JSON.
            #
            # indent=2 makes the file human-readable.
            json.dump(metadata, f, indent=2)

        log.info(
            f"[SAVE] Metadata saved ({len(metadata)} files)"
        )

    except Exception as e:

        # If writing metadata fails, log the error instead of
        # crashing the entire NameNode.
        log.error(
            f"[ERROR] Could not save metadata: {e}"
        )


def load_metadata():
    """
    Load metadata from disk when the NameNode starts.

    If metadata.json doesn't exist, return an empty dictionary.
    """

    # Check whether metadata.json already exists.
    if os.path.exists(METADATA_FILE):

        try:

            # Open the metadata file.
            with open(METADATA_FILE) as f:

                # Convert JSON -> Python dictionary.
                data = json.load(f)

                log.info(
                    f"[LOAD] Metadata loaded ({len(data)} files)"
                )

                return data

        except Exception as e:

            # If the file exists but cannot be parsed/read,
            # log the problem.
            log.error(
                f"[ERROR] Could not load metadata: {e}"
            )

    # No metadata means the NameNode starts with an empty
    # namespace.
    return {}


# ============================================================
# GLOBAL STATE
# ============================================================

# ------------------------------------------------------------
# metadata
# ------------------------------------------------------------
#
# Main NameNode namespace.
#
# Structure:
#
# {
#     "file.txt": [
#         {
#             "chunk_id": 0,
#             "chunk_name": "file.txt.chunk0",
#             "replicas": [
#                 ("localhost", 9002),
#                 ("localhost", 9003)
#             ],
#             "size": 2097152
#         }
#     ]
# }
#
# In simple terms:
#
#       filename -> chunks -> DataNode locations
#
# ------------------------------------------------------------

metadata: Dict[str, List[Dict]] = load_metadata()


# ------------------------------------------------------------
# chunk_locations
# ------------------------------------------------------------
#
# Keeps track of which DataNodes have actually reported
# possession of a particular chunk.
#
# Key:
#
#       (filename, chunk_id)
#
# Value:
#
#       Set of DataNode IDs
#
# Example:
#
# ("file.txt", 0) -> {"dn0", "dn1"}
#
# This is useful when the replication healer checks whether
# enough copies of a chunk exist.
#
# ------------------------------------------------------------

chunk_locations: Dict[Tuple[str, int], Set[str]] = {}


# ------------------------------------------------------------
# heartbeat_table
# ------------------------------------------------------------
#
# Stores the timestamp of the LAST heartbeat received from
# every DataNode.
#
# Example:
#
# {
#     "dn0": 1728300000.2,
#     "dn1": 1728300002.7
# }
#
# If the current time - last heartbeat > timeout,
# the DataNode is considered dead.
#
# ------------------------------------------------------------

heartbeat_table: Dict[str, float] = {
    dn_id: 0.0
    for dn_id in DATANODES.keys()
}


# ------------------------------------------------------------
# Thread synchronization
# ------------------------------------------------------------
#
# The NameNode is multithreaded.
#
# Multiple threads can simultaneously access:
#
#   metadata
#   chunk_locations
#   heartbeat_table
#
# Without a lock, two threads could modify the same data at
# the same time and cause race conditions.
#
# Example:
#
#     Thread A -> modifying metadata
#     Thread B -> reading metadata
#
# state_lock ensures only one critical section accesses the
# shared state at a time.
#
# ------------------------------------------------------------

state_lock = threading.Lock()


# ============================================================
# JSON MESSAGE / NETWORK HELPERS
# ============================================================

# Maximum size allowed for one JSON message.
#
# This prevents an attacker or buggy client from telling us
# to allocate an enormous amount of memory.
#
MAX_JSON_FRAME = 128 * 1024 * 1024


def send_json(conn, obj, who="peer"):
    """
    Send a Python dictionary/object as a framed JSON message.

    TCP is a STREAM protocol.
    
    TCP does not preserve application-level message boundaries.

    Therefore we implement our own framing:
    
        [4-byte message length][JSON data]
    """

    # Convert Python object -> JSON string -> bytes.
    data = json.dumps(obj).encode("utf-8")

    # Pack the length into exactly 4 bytes.
    #
    # >I means:
    #   >  = network byte order / big endian
    #   I  = unsigned 4-byte integer
    #
    header = struct.pack(">I", len(data))

    # Send header + actual JSON message.
    #
    # sendall() ensures all bytes are sent unless an error occurs.
    conn.sendall(header + data)

    log.debug(f"[SEND → {who}] {obj}")


def recv_exact(conn, n):
    """
    Receive exactly n bytes from a TCP connection.

    A single recv() is NOT guaranteed to return all requested
    bytes because TCP is a stream.

    Therefore we repeatedly call recv() until we have all
    required bytes.
    """

    # Buffer where received bytes will be stored.
    buf = b""

    # Continue until exactly n bytes have been received.
    while len(buf) < n:

        chunk = conn.recv(n - len(buf))

        # Empty bytes means the remote side closed the
        # connection.
        if not chunk:
            return None

        buf += chunk

    return buf


def recv_json(conn, who="peer"):
    """
    Receive one framed JSON message.

    First:
        Read 4-byte length.

    Then:
        Read exactly that many bytes.

    Finally:
        Convert JSON -> Python object.
    """

    # First receive the 4-byte message size.
    header = recv_exact(conn, 4)

    if not header:
        return None

    # Convert the 4-byte header back into an integer.
    (length,) = struct.unpack(">I", header)

    # Validate the frame size.
    #
    # This protects against malformed or malicious messages.
    if length <= 0 or length > MAX_JSON_FRAME:
        raise ValueError("Invalid frame length")

    # Receive the actual JSON payload.
    body = recv_exact(conn, length)

    # Decode bytes -> string -> Python dictionary.
    return json.loads(body.decode("utf-8"))


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def live_datanodes_ids():
    """
    Return IDs of DataNodes whose heartbeat is still fresh.

    A DataNode is considered alive if:
    
        current_time - last_heartbeat <= timeout
    """

    now = time.time()

    return [
        dn
        for dn, last in heartbeat_table.items()
        if (now - last) <= HEARTBEAT_TIMEOUT
    ]


def datanode_host_port(dn_id: str) -> Tuple[str, int]:
    """
    Convert a DataNode ID such as "dn0" into its network
    address.

    Example:
    
        "dn0"
          ↓
        ("localhost", 9002)
    """

    info = DATANODES[dn_id]

    return info["host"], int(info["port"])


# ============================================================
# CHUNK PLACEMENT / REPLICATION
# ============================================================

def plan_two_replicas(
    cid: int,
    live_ids: List[str]
) -> List[Tuple[str, int]]:
    """
    Decide where a chunk should be stored.

    The current mini-HDFS assumes two DataNodes:
    
        dn0
        dn1
    
    If both are alive:
    
        Even chunk IDs -> dn0 primary
        Odd chunk IDs  -> dn1 primary
    
    Both DataNodes receive a copy.
    
    This simple alternating strategy provides basic load
    distribution.
    """

    # --------------------------------------------------------
    # CASE 1:
    # Both DataNodes are alive.
    # --------------------------------------------------------

    if set(live_ids) == {"dn0", "dn1"}:

        # Alternate primary DataNode based on chunk ID.
        #
        # chunk 0 -> dn0
        # chunk 1 -> dn1
        # chunk 2 -> dn0
        # chunk 3 -> dn1
        #
        primary = (
            "dn0"
            if cid % 2 == 0
            else "dn1"
        )

        # The other DataNode becomes the secondary replica.
        secondary = (
            "dn1"
            if primary == "dn0"
            else "dn0"
        )

        return [
            datanode_host_port(primary),
            datanode_host_port(secondary)
        ]

    # --------------------------------------------------------
    # CASE 2:
    # Only dn0 is alive.
    #
    # We cannot achieve the desired replication factor,
    # but we can still store the chunk on the available node.
    # --------------------------------------------------------

    elif "dn0" in live_ids:

        return [
            datanode_host_port("dn0")
        ]

    # --------------------------------------------------------
    # CASE 3:
    # Only dn1 is alive.
    # --------------------------------------------------------

    elif "dn1" in live_ids:

        return [
            datanode_host_port("dn1")
        ]

    # --------------------------------------------------------
    # CASE 4:
    # No DataNodes are alive.
    # --------------------------------------------------------

    else:

        return []


# ============================================================
# CLIENT REQUEST HANDLER
# ============================================================
#
# Every incoming TCP client connection gets its own thread.
#
# The client can perform operations such as:
#
#   upload
#   commit_upload
#   download
#   list_files
#   block_report
#
# ============================================================

def handle_client(conn, addr):

    # Create a human-readable client identifier for logging.
    cname = f"{addr[0]}:{addr[1]}"

    try:

        # Read one request from the client.
        req = recv_json(conn, who=cname)

        # If no request was received, simply return.
        if not req:
            return

        # Extract requested operation.
        action = req.get("action")


        # ====================================================
        # UPLOAD REQUEST
        # ====================================================
        #
        # Client tells NameNode:
        #
        # "I want to upload this file.
        #  It contains N chunks."
        #
        # NameNode responds with:
        #
        # "Store chunk 0 here,
        #  chunk 1 there, etc."
        #
        # The actual file data is NOT transferred through the
        # NameNode.
        #
        # ====================================================

        if action in ("upload", "upload_request"):

            # File name supplied by the client.
            filename = req["filename"]

            # Number of chunks into which the client split the
            # file.
            num_chunks = int(req["num_chunks"])

            # Actual sizes of individual chunks.
            #
            # Last chunk is often smaller than the standard
            # chunk size.
            chunk_sizes = req.get("chunk_sizes", [])

            log.info(
                f"[UPLOAD INIT] {filename}, chunks={num_chunks}"
            )


            # Lock shared NameNode state while modifying it.
            with state_lock:

                # Determine currently live DataNodes.
                live = live_datanodes_ids()

                # If no DataNode is available, upload cannot
                # proceed.
                if not live:

                    send_json(
                        conn,
                        {
                            "status": "error",
                            "message": "No live datanodes"
                        }
                    )

                    return


                # ------------------------------------------------
                # Create placement plan.
                # ------------------------------------------------
                #
                # plan will contain information such as:
                #
                # [
                #   {
                #      "chunk_id": 0,
                #      "chunk_name": "...",
                #      "datanodes": [...]
                #   }
                # ]
                #
                # ------------------------------------------------

                plan = []

                for cid in range(num_chunks):

                    # Decide which DataNodes should store
                    # this chunk.
                    endpoints = plan_two_replicas(
                        cid,
                        live
                    )

                    # Add placement information.
                    plan.append(
                        {
                            "chunk_id": cid,

                            "chunk_name":
                                f"{filename}.chunk{cid}",

                            "datanodes": endpoints
                        }
                    )


                # ------------------------------------------------
                # Create NameNode metadata.
                # ------------------------------------------------
                #
                # We overwrite/create the file's metadata.
                #
                # Each chunk gets:
                #
                #   chunk_id
                #   chunk_name
                #   replica locations
                #   chunk size
                #
                # ------------------------------------------------

                metadata[filename] = []

                for cid in range(num_chunks):

                    metadata[filename].append(
                        {
                            "chunk_id": cid,

                            "chunk_name":
                                f"{filename}.chunk{cid}",

                            "replicas":
                                plan[cid]["datanodes"],

                            "size":
                                (
                                    chunk_sizes[cid]
                                    if cid < len(chunk_sizes)
                                    else CHUNK_BYTES
                                )
                        }
                    )


                # Persist the updated metadata.
                save_metadata(metadata)


            # ------------------------------------------------
            # Send placement plan to client.
            # ------------------------------------------------
            #
            # Client now knows where to send each chunk.
            #
            # The actual chunk transfer happens directly
            # between client and DataNodes.
            #
            # ------------------------------------------------

            send_json(
                conn,
                {
                    "status": "ok",
                    "plan": plan
                }
            )


        # ====================================================
        # COMMIT UPLOAD
        # ====================================================
        #
        # The client calls this after completing the upload.
        #
        # This implementation treats the existence of metadata
        # as sufficient to record the commit.
        #
        # ====================================================

        elif action == "commit_upload":

            filename = req["filename"]

            with state_lock:

                # Check whether the file is known to NameNode.
                if filename in metadata:

                    # Persist current metadata.
                    save_metadata(metadata)

                    send_json(
                        conn,
                        {
                            "status": "ok",
                            "message": "commit recorded"
                        }
                    )

                    log.info(
                        f"[COMMIT] {filename} recorded"
                    )

                else:

                    send_json(
                        conn,
                        {
                            "status": "error",
                            "message": "unknown file"
                        }
                    )


        # ====================================================
        # DOWNLOAD REQUEST
        # ====================================================
        #
        # Client asks:
        #
        # "Where can I find the chunks of this file?"
        #
        # NameNode checks which replica DataNodes are alive
        # and returns their locations.
        #
        # ====================================================

        elif action in ("download", "download_request"):

            filename = req["filename"]

            with state_lock:

                # First check whether the file exists.
                if filename in metadata:

                    # Get IDs of currently live DataNodes.
                    live = set(
                        live_datanodes_ids()
                    )

                    result = []


                    # Process every chunk belonging to the file.
                    for rec in metadata[filename]:

                        # This will contain only currently live
                        # replicas.
                        live_repls = []


                        # Check every replica recorded in metadata.
                        for host, port in rec["replicas"]:

                            # Find which DataNode ID corresponds
                            # to this host/port combination.
                            for k, v in DATANODES.items():

                                if (
                                    v["host"] == host
                                    and int(v["port"]) == int(port)
                                    and k in live
                                ):

                                    # This replica is currently
                                    # considered alive.
                                    live_repls.append(
                                        (host, port)
                                    )


                        # ------------------------------------------------
                        # If at least one replica is alive:
                        #
                        #     return live replicas
                        #
                        # Otherwise:
                        #
                        #     return the recorded replicas anyway.
                        #
                        # The latter allows the client to attempt
                        # communication even though the heartbeat
                        # information says the node may be down.
                        # ------------------------------------------------

                        result.append(
                            {
                                "chunk_id":
                                    rec["chunk_id"],

                                "chunk_name":
                                    rec["chunk_name"],

                                "datanodes":
                                    (
                                        live_repls
                                        or rec["replicas"]
                                    ),

                                "size":
                                    rec["size"]
                            }
                        )


                    # Send complete download metadata to client.
                    send_json(
                        conn,
                        {
                            "status": "ok",
                            "metadata": result
                        }
                    )

                else:

                    # File doesn't exist in NameNode namespace.
                    send_json(
                        conn,
                        {
                            "status": "error",
                            "message": "File not found"
                        }
                    )


        # ====================================================
        # LIST FILES
        # ====================================================
        #
        # Returns all filenames known to the NameNode.
        #
        # ====================================================

        elif action == "list_files":

            with state_lock:

                # Extract all filenames from metadata.
                files = list(metadata.keys())


            # Return filenames to client.
            send_json(
                conn,
                {
                    "status": "ok",
                    "files": files
                }
            )


        # ====================================================
        # BLOCK REPORT
        # ====================================================
        #
        # A DataNode periodically tells the NameNode:
        #
        # "These are the chunks I currently have."
        #
        # This helps NameNode maintain knowledge of actual
        # physical chunk locations.
        #
        # ====================================================

        elif action == "block_report":

            # Identify which DataNode sent the report.
            dn_id = req["dn_id"]

            # Get the list of blocks stored on that DataNode.
            blocks = req.get("blocks", [])


            with state_lock:

                # Process every reported block.
                for b in blocks:

                    # Create a unique key for the chunk.
                    #
                    # Example:
                    #
                    # ("file.txt", 2)
                    #
                    key = (
                        b["filename"],
                        int(b["chunk_id"])
                    )


                    # Get existing set of DataNodes for this
                    # chunk.
                    #
                    # If the key doesn't exist, create an empty
                    # set.
                    refs = chunk_locations.setdefault(
                        key,
                        set()
                    )


                    # Record that this DataNode has the chunk.
                    refs.add(dn_id)


            # Tell DataNode that its report was received.
            send_json(
                conn,
                {
                    "status": "ok"
                }
            )

            log.info(
                f"[BLOCK REPORT] {dn_id}: {len(blocks)} blocks"
            )


        # ====================================================
        # UNKNOWN REQUEST
        # ====================================================

        else:

            send_json(
                conn,
                {
                    "status": "error",
                    "message": "unknown action"
                }
            )


    # ========================================================
    # ERROR HANDLING
    # ========================================================

    except Exception as e:

        # Any unexpected client/request error is logged.
        log.error(
            f"[ERROR] {cname}: {e}"
        )


    finally:

        # Always close the TCP connection after handling
        # the request.
        conn.close()


# ============================================================
# CLIENT LISTENER
# ============================================================
#
# This creates the TCP server used by clients.
#
# Flow:
#
# Client
#   |
#   | TCP connection
#   ↓
# NameNode CLIENT_PORT
#   |
#   └──> create new thread
#
# Multiple clients can therefore be handled concurrently.
#
# ============================================================

def client_listener():

    # Create TCP socket.
    with socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    ) as s:

        # Allow immediate reuse of the port after restarting
        # the server.
        s.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        # Bind server to all network interfaces.
        #
        # 0.0.0.0 means:
        # "Accept connections arriving on any interface."
        #
        s.bind(
            ("0.0.0.0", CLIENT_PORT)
        )

        # Put socket into listening mode.
        s.listen()

        log.info(
            f"[CLIENT LISTENER] on {CLIENT_PORT}"
        )


        # Continuously accept incoming clients.
        while True:

            conn, addr = s.accept()

            # Give every client its own thread.
            #
            # daemon=True means the thread won't prevent the
            # Python process from exiting.
            threading.Thread(
                target=handle_client,
                args=(conn, addr),
                daemon=True
            ).start()


# ============================================================
# HEARTBEAT LISTENER
# ============================================================
#
# DataNodes send periodic UDP heartbeat messages.
#
# Example:
#
#     DataNode 0 ---> "dn0" ---> NameNode
#
# UDP is used because heartbeats are small and frequent.
#
# ============================================================

def heartbeat_listener():

    # Create UDP socket.
    with socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    ) as s:

        # Bind to the heartbeat port.
        s.bind(
            ("0.0.0.0", HEARTBEAT_PORT)
        )

        log.info(
            f"[HEARTBEAT LISTENER] on {HEARTBEAT_PORT}"
        )


        # Continuously wait for heartbeat packets.
        while True:

            # Receive UDP packet.
            msg, addr = s.recvfrom(1024)

            # Convert bytes -> string.
            #
            # Example:
            # b"dn0" -> "dn0"
            #
            dn_id = msg.decode().strip()


            with state_lock:

                # Only accept heartbeats from DataNodes that
                # are actually configured.
                if dn_id in heartbeat_table:

                    # Record the current timestamp.
                    heartbeat_table[dn_id] = time.time()


            log.debug(
                f"[HEARTBEAT] from {dn_id}"
            )


# ============================================================
# HEARTBEAT MONITOR
# ============================================================
#
# This thread periodically checks whether DataNodes are alive.
#
# Heartbeat listener:
#     receives heartbeats
#
# Heartbeat monitor:
#     determines whether a heartbeat is too old
#
# ============================================================

def heartbeat_monitor():

    while True:

        # Check every 5 seconds.
        time.sleep(5)

        now = time.time()


        with state_lock:

            # Check every configured DataNode.
            for dn_id, last in heartbeat_table.items():

                # Calculate time since last heartbeat.
                elapsed = now - last


                # If heartbeat is older than the timeout,
                # consider the DataNode unavailable.
                if elapsed > HEARTBEAT_TIMEOUT:

                    log.warning(
                        f"[DOWN] {dn_id} missed heartbeats"
                    )

                else:

                    log.info(
                        f"[ALIVE] {dn_id}"
                    )


# ============================================================
# REPLICATION HEALER
# ============================================================
#
# This thread tries to maintain the desired replication factor.
#
# Suppose:
#
#     replication_factor = 2
#
# Initially:
#
#     chunk0 -> dn0 + dn1
#
# Then dn1 fails.
#
# Now:
#
#     chunk0 -> dn0
#
# Desired:
#
#     chunk0 -> 2 replicas
#
# The healer detects this and finds another live DataNode
# that can become a replica.
#
# ============================================================

def replication_healer():

    while True:

        # Run the healing process every 10 seconds.
        time.sleep(10)


        with state_lock:

            # Determine currently live DataNodes.
            live = set(
                live_datanodes_ids()
            )


            # Iterate over every file.
            for filename, chunks in metadata.items():

                # Iterate over every chunk in that file.
                for rec in chunks:

                    # Create unique chunk identifier.
                    key = (
                        filename,
                        rec["chunk_id"]
                    )


                    # Find DataNodes that reported having
                    # this chunk.
                    #
                    # If no report exists, use an empty set.
                    holders = chunk_locations.get(
                        key,
                        set()
                    )


                    # ------------------------------------------------
                    # Check whether replication factor is satisfied.
                    # ------------------------------------------------
                    #
                    # Example:
                    #
                    # desired = 2
                    # actual  = 1
                    #
                    # missing = 1
                    #
                    # ------------------------------------------------

                    if len(holders) < REPLICATION:

                        # Find DataNodes that:
                        #
                        # 1. Are configured
                        # 2. Are currently alive
                        # 3. Don't already hold the chunk
                        #
                        candidates = [
                            dn
                            for dn in DATANODES.keys()
                            if (
                                dn not in holders
                                and dn in live
                            )
                        ]


                        # Add enough candidates to bring the
                        # replica count toward REPLICATION.
                        #
                        # Example:
                        #
                        # Need 2 replicas
                        # Have 1
                        #
                        # Add:
                        #     2 - 1 = 1
                        #
                        for dn_id in candidates[
                            :REPLICATION - len(holders)
                        ]:

                            # Add the DataNode address to the
                            # metadata.
                            rec["replicas"].append(
                                datanode_host_port(dn_id)
                            )

                            log.info(
                                f"[HEAL] "
                                f"{filename} "
                                f"chunk {rec['chunk_id']} "
                                f"→ added {dn_id}"
                            )


            # Save changes made by the healing process.
            save_metadata(metadata)


# ============================================================
# MAIN
# ============================================================
#
# Start all NameNode services.
#
# The NameNode has four major background responsibilities:
#
# 1. Client listener
# 2. Heartbeat listener
# 3. Heartbeat monitor
# 4. Replication healer
#
# ============================================================

if __name__ == "__main__":

    # Log basic startup information.
    log.info(
        f"[NAMENODE STARTED] "
        f"Host={HOST}, "
        f"ClientPort={CLIENT_PORT}, "
        f"HBPort={HEARTBEAT_PORT}"
    )


    # --------------------------------------------------------
    # Start TCP client server.
    # --------------------------------------------------------
    #
    # Handles:
    #
    #   upload
    #   download
    #   list_files
    #   commit
    #   block reports
    #
    threading.Thread(
        target=client_listener,
        daemon=True
    ).start()


    # --------------------------------------------------------
    # Start UDP heartbeat listener.
    # --------------------------------------------------------
    #
    # Receives periodic "dn0", "dn1", etc.
    #
    threading.Thread(
        target=heartbeat_listener,
        daemon=True
    ).start()


    # --------------------------------------------------------
    # Start heartbeat monitor.
    # --------------------------------------------------------
    #
    # Detects failed/unresponsive DataNodes.
    #
    threading.Thread(
        target=heartbeat_monitor,
        daemon=True
    ).start()


    # --------------------------------------------------------
    # Start replication healer.
    # --------------------------------------------------------
    #
    # Attempts to restore missing replicas.
    #
    threading.Thread(
        target=replication_healer,
        daemon=True
    ).start()


    # --------------------------------------------------------
    # Keep the main NameNode process alive.
    # --------------------------------------------------------
    #
    # The actual work happens in the background threads.
    #
    while True:

        # Sleep so the main thread doesn't continuously consume
        # CPU while keeping the process alive.
        time.sleep(60)
