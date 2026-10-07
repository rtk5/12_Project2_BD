# ============================================================
# DATANODE 1 - MINI HDFS
# ============================================================
#
# This program implements DataNode 1 of our mini-HDFS.
#
# The DataNode is responsible for:
#
#   1. Storing file chunks received from the client
#   2. Receiving replicated chunks
#   3. Sending chunks back when requested
#   4. Sending periodic heartbeats to the NameNode
#   5. Verifying data integrity using checksums
#
# IMPORTANT ARCHITECTURE:
#
#       NameNode
#           |
#           | Metadata / heartbeat
#           |
#     -----------------
#     |               |
#   DN0             DN1
#                     ^
#                     |
#                This program
#
# The NameNode knows WHERE chunks are stored.
# The DataNode actually stores the chunk data.
#
# ============================================================


import os
import socket
import threading
import time
import json
import struct
import logging
import hashlib


# ============================================================
# CONFIGURATION
# ============================================================
#
# Configuration is stored in config.json instead of hardcoding
# ports, directories, heartbeat intervals, etc.
#
# This allows us to run multiple DataNodes with different
# configurations.
#
# ============================================================

def load_config():
    """
    Load the JSON configuration file.

    The function returns the entire configuration as a
    Python dictionary.
    """

    # Open config.json in read mode.
    with open("config.json") as f:

        # Convert JSON -> Python dictionary.
        return json.load(f)


# Load configuration once when the DataNode starts.
config = load_config()


# ============================================================
# LOGGING
# ============================================================
#
# Logging helps us understand what the DataNode is doing.
#
# Examples:
#
#   [START]
#   [HEARTBEAT]
#   [STORE_OK]
#   [GET_OK]
#   [CHECK_FAIL]
#   [INTEGRITY_FAIL]
#
# DEBUG level is enabled here so we can see detailed
# information during development/testing.
#
# ============================================================

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
)

# Create a logger specifically for DataNode 1.
log = logging.getLogger("datanode1")


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def recv_exact(conn, n):
    """
    Receive exactly n bytes from a TCP connection.

    IMPORTANT:
    TCP is a byte-stream protocol.

    A single conn.recv(n) is NOT guaranteed to return all
    n bytes.

    Therefore we keep receiving until exactly n bytes have
    been collected.
    """

    # Buffer used to store received data.
    buf = b""

    # Continue until the required number of bytes arrives.
    while len(buf) < n:

        # Ask TCP socket for the remaining bytes.
        part = conn.recv(n - len(buf))

        # If the connection is closed before receiving all
        # required bytes, return None.
        if not part:
            return None

        # Append received bytes to our buffer.
        buf += part

    return buf


def checksum(data):
    """
    Calculate an MD5 checksum of the given data.

    This is used for DATA INTEGRITY verification.

    Sender:
        calculate checksum(data)

    Receiver:
        calculate checksum(received_data)

    If both values match, the data was very likely received
    without corruption.
    """

    # Convert data -> MD5 hash -> hexadecimal string.
    return hashlib.md5(data).hexdigest()


# ============================================================
# DATANODE 1 MAIN LOGIC
# ============================================================
#
# This function starts DataNode 1.
#
# Parameters:
#
#   node_id
#       Unique DataNode identifier.
#       Example: "dn1"
#
#   host
#       IP/hostname on which this DataNode listens.
#
#   port
#       TCP port used for chunk operations.
#
#   storage_dir
#       Directory where actual chunks are stored.
#
#   namenode_host
#       Hostname/IP of the NameNode.
#
#   namenode_heartbeat_port
#       UDP port on which NameNode receives heartbeats.
#
# ============================================================

def datanode_1(
    node_id,
    host,
    port,
    storage_dir,
    namenode_host,
    namenode_heartbeat_port
):

    # --------------------------------------------------------
    # Create storage directory if it doesn't already exist.
    #
    # exist_ok=True means:
    #
    #   Directory exists -> do nothing
    #   Directory doesn't exist -> create it
    #
    # This is where actual chunks will be stored.
    # --------------------------------------------------------

    os.makedirs(
        storage_dir,
        exist_ok=True
    )


    # Log DataNode startup information.
    log.info(
        f"[START] Datanode1 (replica) "
        f"{node_id} running at {host}:{port}"
    )


    # ========================================================
    # HEARTBEAT THREAD
    # ========================================================
    #
    # The NameNode needs to know whether this DataNode is alive.
    #
    # Therefore this DataNode periodically sends a heartbeat:
    #
    #       DataNode 1
    #            |
    #            | UDP: "dn1"
    #            ↓
    #       NameNode
    #
    # UDP is used because heartbeats are small and frequent.
    #
    # ========================================================

    def send_heartbeat():

        # Continue sending heartbeats for the lifetime of
        # the DataNode.
        while True:

            try:

                # ------------------------------------------------
                # Create a UDP socket.
                #
                # SOCK_DGRAM = UDP
                # ------------------------------------------------

                s = socket.socket(
                    socket.AF_INET,
                    socket.SOCK_DGRAM
                )


                # ------------------------------------------------
                # Send DataNode ID to the NameNode.
                #
                # Example:
                #
                #       "dn1"
                #
                # The NameNode uses this ID to update its
                # heartbeat table.
                # ------------------------------------------------

                s.sendto(
                    node_id.encode(),
                    (
                        namenode_host,
                        namenode_heartbeat_port
                    )
                )


                # Close UDP socket after sending heartbeat.
                s.close()


                log.debug(
                    f"[HEARTBEAT] "
                    f"Sent heartbeat from {node_id}"
                )


            except Exception as e:

                # If heartbeat transmission fails, log it.
                #
                # We don't terminate the DataNode because a
                # temporary network error shouldn't necessarily
                # kill the storage server.
                log.error(
                    f"[HEARTBEAT_FAIL] {e}"
                )


            # ------------------------------------------------
            # Wait until the next heartbeat.
            #
            # The interval comes from config.json.
            # ------------------------------------------------

            time.sleep(
                config["heartbeat_interval_sec"]
            )


    # ========================================================
    # CHUNK STORAGE AND RETRIEVAL
    # ========================================================
    #
    # This function starts the TCP server used for actual
    # chunk operations.
    #
    # Supported commands:
    #
    #       REPLICATE
    #       STORE
    #       GET
    #
    # STORE / REPLICATE:
    #
    #       Client/DataNode -> DataNode
    #
    #       Send chunk data to be stored.
    #
    # GET:
    #
    #       Client -> DataNode
    #
    #       Request a previously stored chunk.
    #
    # ========================================================

    def listen_for_chunks():

        # --------------------------------------------------------
        # Create TCP socket.
        #
        # AF_INET      -> IPv4
        # SOCK_STREAM  -> TCP
        # --------------------------------------------------------

        s = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )


        # --------------------------------------------------------
        # Allow the socket to reuse its port.
        #
        # This is useful when restarting the DataNode quickly
        # after stopping it.
        # --------------------------------------------------------

        s.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )


        # --------------------------------------------------------
        # Bind the server to the configured host and port.
        #
        # Example:
        #
        #       localhost:9102
        #
        # --------------------------------------------------------

        s.bind(
            (host, port)
        )


        # Put the socket into listening mode.
        s.listen()


        log.info(
            f"[LISTEN] Datanode1 ready on {host}:{port}"
        )


        # --------------------------------------------------------
        # Continuously accept incoming connections.
        # --------------------------------------------------------

        while True:

            # Wait for a client/DataNode to connect.
            conn, addr = s.accept()


            try:

                # =================================================
                # READ COMMAND
                # =================================================
                #
                # Our protocol starts with a text command terminated
                # by a newline.
                #
                # Example:
                #
                #       STORE\n
                #       REPLICATE\n
                #       GET\n
                #
                # We read one byte at a time until '\n'.
                #
                # =================================================

                cmd = b""


                while not cmd.endswith(b"\n"):

                    # Read one byte.
                    part = conn.recv(1)


                    # Connection closed before command finished.
                    if not part:
                        break


                    # Add byte to command buffer.
                    cmd += part


                # Convert:
                #
                #       b"GET\n"
                #
                # into:
                #
                #       "GET"
                #
                cmd = cmd.decode().strip()


                log.debug(
                    f"[CMD] Received command "
                    f"'{cmd}' from {addr}"
                )


                # =================================================
                # READ HEADER LENGTH
                # =================================================
                #
                # After the command, the sender sends:
                #
                #       4-byte header length
                #
                # followed by:
                #
                #       JSON header
                #
                # Therefore the wire format is:
                #
                #   [COMMAND]\n
                #   [4-byte header length]
                #   [JSON header]
                #   [DATA]
                #
                # =================================================

                hdr_len_raw = recv_exact(
                    conn,
                    4
                )


                # If the 4-byte header length wasn't received,
                # the request is incomplete.
                if not hdr_len_raw:

                    log.error(
                        "[HEADER_ERROR] "
                        "Incomplete header length"
                    )

                    conn.close()

                    continue


                # Convert the 4-byte integer into Python int.
                #
                # >I:
                #   > = big endian
                #   I = unsigned 4-byte integer
                #
                hdr_len = struct.unpack(
                    ">I",
                    hdr_len_raw
                )[0]


                # =================================================
                # READ JSON HEADER
                # =================================================
                #
                # Now that we know the header size, receive
                # exactly that many bytes.
                #
                # =================================================

                header_raw = recv_exact(
                    conn,
                    hdr_len
                )


                # If complete header wasn't received, abort
                # this request.
                if not header_raw:

                    log.error(
                        "[HEADER_ERROR] "
                        "Incomplete header"
                    )

                    conn.close()

                    continue


                # Convert:
                #
                #       bytes -> string -> JSON -> Python dict
                #
                header = json.loads(
                    header_raw.decode()
                )


                # =================================================
                # STORE / REPLICATE
                # =================================================
                #
                # Both commands mean:
                #
                # "Receive this chunk and store it locally."
                #
                # STORE:
                #     Normal storage operation.
                #
                # REPLICATE:
                #     Store a replica copied from another node.
                #
                # =================================================

                if cmd in ("REPLICATE", "STORE"):


                    # ------------------------------------------------
                    # Extract chunk information from the header.
                    # ------------------------------------------------

                    # Name of the chunk file.
                    chunk_name = header["chunk_name"]

                    # Number of bytes expected.
                    size = header["size"]

                    # Optional checksum supplied by sender.
                    checksum_ref = header.get("checksum")


                    # ------------------------------------------------
                    # Send READY handshake.
                    #
                    # This tells the sender:
                    #
                    # "I have received your header and I'm ready
                    #  to receive the actual chunk data."
                    #
                    # ------------------------------------------------

                    conn.sendall(
                        b"READY"
                    )


                    # =================================================
                    # RECEIVE CHUNK DATA
                    # =================================================
                    #
                    # The header told us exactly how many bytes
                    # should arrive.
                    #
                    # We continue reading until:
                    #
                    #       len(data) == size
                    #
                    # =================================================

                    data = b""


                    while len(data) < size:

                        # Receive at most 4096 bytes at a time.
                        #
                        # min() ensures we never receive more than
                        # the remaining expected data.
                        block = conn.recv(
                            min(
                                4096,
                                size - len(data)
                            )
                        )


                        # Sender closed connection unexpectedly.
                        if not block:
                            break


                        # Append received bytes.
                        data += block


                    # =================================================
                    # CHECKSUM VERIFICATION
                    # =================================================
                    #
                    # We calculate the checksum of the data we
                    # actually received.
                    #
                    # Sender:
                    #
                    #       MD5(original_data)
                    #
                    # Receiver:
                    #
                    #       MD5(received_data)
                    #
                    # If they differ, data may have been corrupted.
                    #
                    # =================================================

                    local_sum = checksum(data)


                    # Only perform comparison if sender actually
                    # supplied a reference checksum.
                    if (
                        checksum_ref
                        and local_sum != checksum_ref
                    ):

                        log.warning(
                            f"[CHECK_FAIL] "
                            f"{chunk_name} corrupted during transfer! "
                            f"REF={checksum_ref} "
                            f"LOCAL={local_sum}"
                        )


                        # Tell sender that integrity verification
                        # failed.
                        conn.sendall(
                            b"ERROR:CHECKSUM_FAIL"
                        )


                        # Do NOT store corrupted data.
                        continue


                    # =================================================
                    # WRITE CHUNK TO LOCAL STORAGE
                    # =================================================
                    #
                    # Each chunk is stored as a separate file.
                    #
                    # Example:
                    #
                    #       storage_dir/
                    #           file.txt.chunk0
                    #           file.txt.chunk1
                    #
                    # =================================================

                    path = os.path.join(
                        storage_dir,
                        chunk_name
                    )


                    # Open file in binary write mode.
                    #
                    # "wb":
                    #     w = write
                    #     b = binary
                    #
                    with open(path, "wb") as f:

                        # Write the received chunk data.
                        f.write(data)


                    # Log successful storage.
                    log.info(
                        f"[STORE_OK] "
                        f"Stored {chunk_name} "
                        f"({len(data)} bytes) "
                        f"MD5={local_sum}"
                    )


                # =================================================
                # GET CHUNK
                # =================================================
                #
                # Client asks:
                #
                #       "Give me chunk X."
                #
                # DataNode:
                #
                #       1. Checks whether chunk exists.
                #       2. Reads chunk from disk.
                #       3. Verifies checksum if provided.
                #       4. Sends chunk back.
                #
                # =================================================

                elif cmd == "GET":


                    # Get requested chunk name.
                    chunk_name = header["chunk_name"]


                    # Build complete path to chunk.
                    path = os.path.join(
                        storage_dir,
                        chunk_name
                    )


                    # ------------------------------------------------
                    # Check whether the requested chunk exists.
                    # ------------------------------------------------

                    if not os.path.isfile(path):

                        log.error(
                            f"[GET_ERROR] "
                            f"Missing replica {chunk_name}"
                        )


                        # Inform client that the chunk does not
                        # exist on this DataNode.
                        conn.sendall(
                            b"ERROR:NOT_FOUND"
                        )

                        continue


                    # ------------------------------------------------
                    # Read chunk from disk.
                    # ------------------------------------------------

                    with open(path, "rb") as f:

                        # Read entire chunk into memory.
                        data = f.read()


                    # Calculate checksum of stored data.
                    stored_sum = checksum(data)


                    # =================================================
                    # OPTIONAL INTEGRITY VERIFICATION
                    # =================================================
                    #
                    # If the requester supplied a checksum,
                    # compare it with the checksum of the local
                    # stored chunk.
                    #
                    # This allows us to detect corruption that may
                    # have happened while the chunk was sitting on
                    # disk.
                    #
                    # =================================================

                    if (
                        "checksum" in header
                        and header["checksum"] != stored_sum
                    ):

                        log.error(
                            f"[INTEGRITY_FAIL] "
                            f"Replica {chunk_name} "
                            f"MD5 mismatch on GET"
                        )


                        # Don't send potentially corrupted data.
                        conn.sendall(
                            b"ERROR:INTEGRITY_FAIL"
                        )

                        continue


                    # ------------------------------------------------
                    # Send actual chunk data to requester.
                    # ------------------------------------------------

                    conn.sendall(
                        data
                    )


                    # Log successful download/transfer.
                    log.info(
                        f"[GET_OK] "
                        f"Sent {chunk_name} "
                        f"({len(data)} bytes) "
                        f"MD5={stored_sum}"
                    )


                # =================================================
                # UNKNOWN COMMAND
                # =================================================

                else:

                    log.error(
                        f"[CMD_ERROR] "
                        f"Unknown command '{cmd}'"
                    )


            # ========================================================
            # REQUEST ERROR HANDLING
            # ========================================================

            except Exception as e:

                # log.exception() prints both the error message
                # and the traceback, which is useful for debugging.
                log.exception(
                    f"[ERROR] {e}"
                )


            finally:

                # Regardless of success/failure, close the
                # client connection.
                conn.close()


    # ============================================================
    # START HEARTBEAT THREAD
    # ============================================================
    #
    # This runs send_heartbeat() independently of the TCP server.
    #
    # Therefore:
    #
    #       Heartbeats can continue
    #       while DataNode handles client requests.
    #
    # ============================================================

    threading.Thread(
        target=send_heartbeat,
        daemon=True,
        name="dn1-heartbeat"
    ).start()


    # ============================================================
    # START CHUNK SERVER
    # ============================================================
    #
    # This function continuously listens for STORE/REPLICATE/GET
    # requests.
    #
    # It runs in the main DataNode thread.
    #
    # ============================================================

    listen_for_chunks()


# ============================================================
# ENTRY POINT
# ============================================================
#
# Python starts execution here when this file is run directly:
#
#       python datanode1.py
#
# ============================================================

if __name__ == "__main__":


    # ------------------------------------------------------------
    # Get DataNode 1's configuration.
    #
    # Example:
    #
    # config["datanodes"]["dn1"]
    #
    # might contain:
    #
    # {
    #     "host": "localhost",
    #     "port": 9102,
    #     "storage_dir": "./storage/dn1"
    # }
    #
    # ------------------------------------------------------------

    cfg = config["datanodes"]["dn1"]


    # Get NameNode configuration.
    #
    # This contains:
    #
    #   NameNode host
    #   heartbeat port
    #
    nn = config["namenode"]


    # ------------------------------------------------------------
    # Start DataNode 1.
    #
    # Arguments:
    #
    #   "dn1"
    #       Unique DataNode ID
    #
    #   cfg["host"]
    #       DataNode host
    #
    #   cfg["port"]
    #       DataNode TCP port
    #
    #   cfg["storage_dir"]
    #       Local directory for chunks
    #
    #   nn["host"]
    #       NameNode host
    #
    #   nn["heartbeat_port"]
    #       NameNode UDP heartbeat port
    #
    # ------------------------------------------------------------

    datanode_1(
        "dn1",
        cfg["host"],
        cfg["port"],
        cfg["storage_dir"],
        nn["host"],
        nn["heartbeat_port"]
    )
