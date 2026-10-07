# ============================================================
# MINI-HDFS CLIENT
# ============================================================
#
# This program acts as the CLIENT of our mini-HDFS system.
#
# The client is responsible for:
#
#   1. Communicating with the NameNode
#   2. Splitting files into chunks
#   3. Uploading chunks to DataNodes
#   4. Downloading chunks from DataNodes
#   5. Reconstructing downloaded files
#   6. Providing a web-based dashboard using Flask
#   7. Displaying logs and system status
#
#
# IMPORTANT ARCHITECTURE:
#
#                    ┌──────────────┐
#                    │    CLIENT    │
#                    └──────┬───────┘
#                           │
#              metadata     │
#                           ▼
#                    ┌──────────────┐
#                    │   NAMENODE   │
#                    └──────┬───────┘
#                           │
#                chunk locations
#                           │
#              ┌────────────┴────────────┐
#              ▼                         ▼
#        ┌───────────┐             ┌───────────┐
#        │ DataNode0 │             │ DataNode1 │
#        └───────────┘             └───────────┘
#
#
# The NameNode tells the client WHERE chunks should go.
# The client then transfers the actual chunk DATA directly
# to the DataNodes.
#
# ============================================================


import socket
import json
import os
import hashlib
import struct
import logging
import threading
import time

# Flask is used to provide a web dashboard.
from flask import Flask, render_template_string, request, jsonify

# Thread is used to run upload/download/dashboard operations
# without blocking the main program.
from threading import Thread


# ============================================================
# LOGGING SETUP
# ============================================================
#
# Logging allows us to monitor what the client is doing.
#
# DEBUG level means detailed information will be displayed.
#
# Example:
#
#   [SEND]
#   [RECV]
#   [UPLOAD]
#   [DOWNLOAD]
#
# ============================================================

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | CLIENT | %(message)s"
)

# Create a logger specifically for the client.
log = logging.getLogger("client")


# ============================================================
# LOAD CONFIGURATION FILE
# ============================================================
#
# Instead of hardcoding the NameNode address and chunk size,
# we read them from config.json.
#
# ============================================================

with open("config.json") as f:

    # Convert JSON configuration into a Python dictionary.
    CONFIG = json.load(f)


# ------------------------------------------------------------
# Extract NameNode configuration.
# ------------------------------------------------------------

# Host/IP address where the NameNode is running.
NAMENODE_HOST = CONFIG["namenode"]["host"]

# TCP port used by the NameNode for client communication.
NAMENODE_PORT = CONFIG["namenode"]["client_port"]

# Chunk size is specified in MB in config.json.
#
# Convert MB -> bytes because file operations work with bytes.
#
# Example:
#
#   2 MB
#   ↓
#   2 * 1024 * 1024 bytes
#
CHUNK_SIZE = int(
    CONFIG["chunk_size_mb"]
) * 1024 * 1024


# ============================================================
# FRAMED JSON HELPERS
# ============================================================
#
# Our client communicates with the NameNode using JSON.
#
# However, TCP is a BYTE STREAM and doesn't preserve message
# boundaries.
#
# Therefore we use:
#
#       [4-byte message length][JSON payload]
#
# This is called MESSAGE FRAMING.
#
# ============================================================


def send_json(sock, obj, who="namenode"):
    """
    Send a Python object as a framed JSON message.
    """

    # Convert Python dictionary -> JSON string -> bytes.
    data = json.dumps(obj).encode("utf-8")

    # Store the JSON payload size in a 4-byte integer.
    #
    # >I means:
    #
    #   > = big endian / network byte order
    #   I = unsigned 4-byte integer
    #
    header = struct.pack(
        ">I",
        len(data)
    )

    # Send:
    #
    #   4-byte length
    #   +
    #   JSON data
    #
    sock.sendall(
        header + data
    )

    log.debug(
        f"[SEND → {who}] "
        f"{len(data)} bytes | {obj}"
    )


def recv_exact(sock, n):
    """
    Receive exactly n bytes from a TCP socket.

    We cannot assume one recv() call returns all n bytes.

    Therefore we repeatedly receive until the requested
    number of bytes has been collected.
    """

    # Buffer for received bytes.
    buf = b""

    # Keep receiving until exactly n bytes are available.
    while len(buf) < n:

        # Ask socket for the remaining bytes.
        chunk = sock.recv(
            n - len(buf)
        )

        # Empty bytes means the connection was closed.
        if not chunk:
            return None

        # Append newly received bytes.
        buf += chunk

    return buf


def recv_json(sock, who="namenode"):
    """
    Receive one framed JSON message from the socket.
    """

    # --------------------------------------------------------
    # First receive the 4-byte message length.
    # --------------------------------------------------------

    hdr = recv_exact(
        sock,
        4
    )

    if not hdr:
        return None


    # Convert 4-byte header into integer.
    (length,) = struct.unpack(
        ">I",
        hdr
    )


    # --------------------------------------------------------
    # Now receive exactly 'length' bytes.
    # --------------------------------------------------------

    body = recv_exact(
        sock,
        length
    )

    if not body:
        return None


    # Convert:
    #
    # bytes -> string -> JSON -> Python object
    #
    obj = json.loads(
        body.decode("utf-8")
    )


    log.debug(
        f"[RECV ← {who}] {obj}"
    )

    return obj


# ============================================================
# NAME NODE COMMUNICATION
# ============================================================


def send_to_namenode(message):
    """
    Send a request to the NameNode and wait for its response.

    This function is used for operations such as:
    
        upload_request
        commit_upload
        download_request
    
    The NameNode handles metadata.
    """

    # Create a TCP socket.
    s = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    try:

        log.info(
            f"Connecting to Namenode "
            f"{NAMENODE_HOST}:{NAMENODE_PORT} ..."
        )


        # Establish TCP connection to NameNode.
        s.connect(
            (
                NAMENODE_HOST,
                NAMENODE_PORT
            )
        )


        # Send JSON request.
        send_json(
            s,
            message,
            who="namenode"
        )


        # Wait for NameNode response.
        return recv_json(
            s,
            who="namenode"
        )


    except Exception as e:

        # If NameNode communication fails, log the error.
        log.error(
            f"Error communicating with Namenode: {e}"
        )

        return None


    finally:

        # Always close the connection.
        s.close()


# ============================================================
# UPLOAD HELPERS
# ============================================================


def split_file(filename):
    """
    Split a file into fixed-size chunks.

    Returns:
    
        chunks
        checksums
    
    Example:
    
        5 MB file
        chunk size = 2 MB
    
        chunk0 = 2 MB
        chunk1 = 2 MB
        chunk2 = 1 MB
    """

    # List that will contain actual chunk data.
    chunks = []

    # List that will contain MD5 checksum for each chunk.
    checksums = []


    # Open the file in binary read mode.
    with open(filename, "rb") as f:

        while True:

            # Read at most CHUNK_SIZE bytes.
            chunk = f.read(
                CHUNK_SIZE
            )


            # If no data is returned, we've reached EOF.
            if not chunk:
                break


            # Store the actual chunk.
            chunks.append(
                chunk
            )


            # Calculate MD5 checksum of this chunk.
            #
            # This can be used to verify data integrity.
            checksums.append(
                hashlib.md5(chunk).hexdigest()
            )


    return chunks, checksums


# ============================================================
# SEND ONE CHUNK TO A DATANODE
# ============================================================
#
# This function performs the actual data transfer.
#
# The NameNode has already told us which DataNode should
# receive the chunk.
#
# ============================================================

def send_chunk(
    target_host,
    target_port,
    chunk_name,
    data,
    chunk_index,
    total_chunks
):

    # Create TCP socket.
    s = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    try:

        log.info(
            f"Uploading chunk "
            f"{chunk_index+1}/{total_chunks} "
            f"({chunk_name}) "
            f"to {target_host}:{target_port}"
        )


        # Connect directly to the DataNode.
        #
        # IMPORTANT:
        #
        # The actual file data is NOT sent through the
        # NameNode.
        #
        # Client -> DataNode
        #
        s.connect(
            (
                target_host,
                target_port
            )
        )


        # --------------------------------------------------------
        # Send STORE command.
        #
        # DataNode expects:
        #
        #       STORE\n
        #
        # --------------------------------------------------------

        s.sendall(
            b"STORE\n"
        )


        # --------------------------------------------------------
        # Construct metadata header.
        #
        # The DataNode needs to know:
        #
        #   chunk name
        #   chunk size
        #
        # --------------------------------------------------------

        header = {
            "chunk_name": chunk_name,
            "size": len(data)
        }


        # Convert header dictionary -> JSON -> bytes.
        payload = json.dumps(
            header
        ).encode("utf-8")


        # --------------------------------------------------------
        # Send:
        #
        #       4-byte JSON length
        #       JSON header
        #
        # --------------------------------------------------------

        s.sendall(
            struct.pack(
                ">I",
                len(payload)
            ) + payload
        )


        # --------------------------------------------------------
        # Wait for DataNode handshake.
        #
        # DataNode should respond:
        #
        #       READY
        #
        # This means:
        #
        # "I received the header and am ready for data."
        # --------------------------------------------------------

        ack = s.recv(32)


        if ack != b"READY":

            log.error(
                f"Datanode didn't acknowledge "
                f"READY for {chunk_name}"
            )

            return


        # --------------------------------------------------------
        # Send actual chunk data.
        # --------------------------------------------------------

        s.sendall(
            data
        )


        log.info(
            f"✅ Sent {chunk_name} successfully"
        )


    except Exception as e:

        log.error(
            f"Error sending chunk "
            f"{chunk_name}: {e}"
        )


    finally:

        # Close connection after chunk transfer.
        s.close()


# ============================================================
# COMPLETE FILE UPLOAD
# ============================================================


def upload_file(filename):
    """
    Upload an entire file to the mini-HDFS.

    High-level flow:

        1. Check file exists
        2. Split file into chunks
        3. Ask NameNode for placement plan
        4. Send chunks to DataNodes
        5. Commit upload with NameNode
    """

    # --------------------------------------------------------
    # Check whether local file exists.
    # --------------------------------------------------------

    if not os.path.exists(filename):

        log.error(
            f"File not found: {filename}"
        )

        return


    # --------------------------------------------------------
    # Split file into chunks.
    # --------------------------------------------------------

    chunks, checksums = split_file(
        filename
    )


    # Number of chunks created.
    num_chunks = len(chunks)


    log.info(
        f"Split into {num_chunks} chunks"
    )


    # ========================================================
    # ASK NAMENODE FOR PLACEMENT PLAN
    # ========================================================
    #
    # We don't decide ourselves where chunks should go.
    #
    # The NameNode knows:
    #
    #   - which DataNodes are alive
    #   - replication requirements
    #   - chunk placement
    #
    # ========================================================

    resp = send_to_namenode(
        {
            "action": "upload_request",

            # Only filename is sent, not entire file data.
            "filename": os.path.basename(filename),

            # Tell NameNode how many chunks exist.
            "num_chunks": num_chunks
        }
    )


    # If NameNode didn't respond successfully, stop upload.
    if (
        not resp
        or resp.get("status") != "ok"
    ):

        log.error(
            "Upload request failed."
        )

        return


    # --------------------------------------------------------
    # Extract placement plan returned by NameNode.
    # --------------------------------------------------------

    plan = resp["plan"]


    # ========================================================
    # SEND EACH CHUNK TO ITS REPLICAS
    # ========================================================

    for i, chunk_info in enumerate(plan):

        # Get actual chunk data from our local chunks list.
        data = chunks[i]


        # NameNode may return multiple DataNodes because of
        # replication.
        #
        # Example:
        #
        # chunk0 -> dn0 + dn1
        #
        for host, port in chunk_info["datanodes"]:

            # Send the SAME chunk to every replica DataNode.
            send_chunk(
                host,
                port,
                chunk_info["chunk_name"],
                data,
                i,
                num_chunks
            )


    # ========================================================
    # COMMIT UPLOAD
    # ========================================================
    #
    # After all chunks have been sent, tell NameNode:
    #
    #       "The upload is complete."
    #
    # ========================================================

    commit = send_to_namenode(
        {
            "action": "commit_upload",
            "filename": os.path.basename(filename)
        }
    )


    if (
        commit
        and commit.get("status") == "ok"
    ):

        log.info(
            "✅ Upload complete and committed."
        )

    else:

        log.warning(
            "Upload complete but commit not confirmed."
        )


# ============================================================
# DOWNLOAD HELPERS
# ============================================================


def download_file(filename, output_path):
    """
    Download a file from mini-HDFS.

    High-level flow:

        1. Ask NameNode for chunk metadata
        2. Get location of each chunk
        3. Download chunks from DataNodes
        4. Write chunks sequentially
        5. Reconstruct original file
    """

    # ========================================================
    # ASK NAMENODE WHERE THE FILE'S CHUNKS ARE
    # ========================================================

    meta = send_to_namenode(
        {
            "action": "download_request",
            "filename": filename
        }
    )


    # If NameNode doesn't know about the file, stop.
    if (
        not meta
        or meta.get("status") != "ok"
    ):

        log.error(
            "Download request failed."
        )

        return


    # Extract chunk metadata.
    chunks_meta = meta["metadata"]


    # --------------------------------------------------------
    # Create output file.
    #
    # "wb" means:
    #
    #   write + binary
    #
    # Chunks will be written sequentially.
    # --------------------------------------------------------

    with open(
        output_path,
        "wb"
    ) as out:


        # Process each chunk in metadata order.
        for chunk in chunks_meta:


            # ------------------------------------------------
            # Select the first available DataNode returned
            # by NameNode.
            # ------------------------------------------------

            host, port = chunk["datanodes"][0]


            # Create TCP connection to DataNode.
            s = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )


            try:

                # Connect directly to DataNode.
                s.connect(
                    (
                        host,
                        port
                    )
                )


                # ------------------------------------------------
                # Send GET command.
                # ------------------------------------------------

                s.sendall(
                    b"GET\n"
                )


                # ------------------------------------------------
                # Tell DataNode which chunk we want.
                # ------------------------------------------------

                header = {
                    "chunk_name":
                        chunk["chunk_name"]
                }


                # Convert header to JSON bytes.
                data = json.dumps(
                    header
                ).encode("utf-8")


                # Send:
                #
                #   4-byte length
                #   JSON header
                #
                s.sendall(
                    struct.pack(
                        ">I",
                        len(data)
                    ) + data
                )


                # ------------------------------------------------
                # Receive entire chunk.
                #
                # We don't know exactly how many recv() calls
                # will be required, so continue until the
                # DataNode closes the connection.
                # ------------------------------------------------

                buf = b""


                while True:

                    # Receive up to 64 KB at a time.
                    packet = s.recv(
                        65536
                    )


                    # Empty packet means DataNode closed
                    # the connection.
                    if not packet:
                        break


                    # Append received bytes.
                    buf += packet


                # ------------------------------------------------
                # Write the reconstructed chunk to the output
                # file.
                #
                # Because chunks are processed in order, the
                # original file is reconstructed correctly.
                # ------------------------------------------------

                out.write(
                    buf
                )


            finally:

                # Close DataNode connection.
                s.close()


    log.info(
        f"✅ File reconstructed as {output_path}"
    )


# ============================================================
# SYSTEM STATUS
# ============================================================
#
# This function asks the NameNode for overall cluster
# information and converts the response into human-readable
# text for the dashboard.
#
# ============================================================

def get_system_status():

    # Ask NameNode for system status.
    status = send_to_namenode(
        {
            "action": "system_status"
        }
    )


    # If NameNode didn't respond:
    if not status:

        return (
            "⚠️ Unable to retrieve system status."
        )


    # List used to build formatted status output.
    lines = []


    lines.append(
        "=== SYSTEM STATUS ==="
    )


    # ========================================================
    # DATANODE STATUS
    # ========================================================

    if "nodes" in status:

        lines.append(
            "Healthy Datanodes:"
        )


        # Process every DataNode.
        for dn, info in status["nodes"].items():

            # Green if alive.
            #
            # Red if not alive.
            state = (
                "🟢"
                if info.get("alive")
                else "🔴"
            )


            lines.append(
                f"  {state} "
                f"{dn} "
                f"({info.get('host')}:{info.get('port')})"
            )


    # ========================================================
    # FILE DISTRIBUTION
    # ========================================================

    if "files" in status:

        lines.append(
            "\n=== FILE DISTRIBUTION ==="
        )


        # Iterate over every file.
        for fname, chunks in status["files"].items():

            lines.append(
                f"{fname}:"
            )


            # Show where every chunk is stored.
            for ch in chunks:

                lines.append(
                    f"  "
                    f"{ch['chunk_name']} "
                    f"-> {ch['datanodes']}"
                )


    # ========================================================
    # FILE INTEGRITY
    # ========================================================

    if "integrity" in status:

        lines.append(
            "\n=== FILE INTEGRITY ==="
        )


        # Process integrity results.
        for f, res in status["integrity"].items():

            # Checkmark if valid.
            #
            # Cross if integrity failed.
            ok = (
                "✅"
                if res
                else "❌"
            )


            lines.append(
                f"  {ok} {f}"
            )


    # Convert list of lines into one string.
    return "\n".join(lines)


# ============================================================
# FLASK DASHBOARD
# ============================================================
#
# Flask provides a simple web interface for our mini-HDFS.
#
# Instead of interacting only through terminal commands,
# users can open:
#
#       http://localhost:8080
#
# and interact with the system through a browser.
#
# ============================================================

app = Flask(
    __name__
)


# ------------------------------------------------------------
# Stores recent log messages so the dashboard can display them.
# ------------------------------------------------------------

LOG_BUFFER = []


class LogCaptureHandler(logging.Handler):
    """
    Custom logging handler.

    Instead of only printing logs to the terminal,
    this handler also stores recent logs in LOG_BUFFER.

    The dashboard then displays those logs.
    """

    def emit(self, record):

        # Convert logging record into formatted text.
        msg = self.format(
            record
        )


        # Add log message to buffer.
        LOG_BUFFER.append(
            msg
        )


        # --------------------------------------------------------
        # Keep only the latest 200 log messages.
        #
        # This prevents the buffer from growing indefinitely.
        # --------------------------------------------------------

        if len(LOG_BUFFER) > 200:

            LOG_BUFFER.pop(
                0
            )


# Attach our custom handler to the client logger.
log.addHandler(
    LogCaptureHandler()
)


# ============================================================
# HTML DASHBOARD
# ============================================================
#
# This HTML page is served directly by Flask.
#
# It contains:
#
#   - Upload form
#   - Download form
#   - Log display
#
# Bootstrap is used for simple styling.
#
# ============================================================

HTML_PAGE = """
<!DOCTYPE html>

<html>

<head>

  <title>Storage Client Dashboard</title>

  <!-- Bootstrap CSS for styling -->
  <link
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
    rel="stylesheet"
  >

</head>


<body class="bg-dark text-light p-4">

  <!-- Dashboard title -->

  <h1>
    📦 Storage Client Dashboard
  </h1>


  <!-- =====================================================
       UPLOAD FORM
       ===================================================== -->

  <form
    id="uploadForm"
    enctype="multipart/form-data"
  >

    <!-- File selection -->

    <input
      type="file"
      name="file"
      class="form-control my-2"
      required
    >


    <!-- Upload button -->

    <button
      type="button"
      class="btn btn-success w-100"
      onclick="uploadFile()"
    >
      Upload
    </button>

  </form>


  <!-- =====================================================
       DOWNLOAD FORM
       ===================================================== -->

  <form
    id="downloadForm"
    class="mt-3"
  >

    <!-- Filename to download -->

    <input
      type="text"
      name="filename"
      placeholder="Filename to download"
      class="form-control my-2"
      required
    >


    <!-- Download button -->

    <button
      type="button"
      class="btn btn-primary w-100"
      onclick="downloadFile()"
    >
      Download
    </button>

  </form>


  <!-- =====================================================
       LOG DISPLAY
       ===================================================== -->

  <pre
    id="logArea"
    class="bg-black text-success mt-3 p-3"
    style="
      height:400px;
      overflow:auto;
      white-space:pre-wrap;
    "
  ></pre>


  <!-- =====================================================
       JAVASCRIPT
       ===================================================== -->

  <script>


    // ------------------------------------------------------
    // Generic API request helper.
    // ------------------------------------------------------
    //
    // Sends a request to one of our Flask endpoints.
    //
    // ------------------------------------------------------

    async function apiRequest(path, opts){

      const res = await fetch(
        path,
        opts
      );


      // If HTTP request failed, throw an error.
      if (!res.ok)
        throw new Error(
          'Network error'
        );


      return res;
    }


    // ------------------------------------------------------
    // Upload file from browser.
    // ------------------------------------------------------

    async function uploadFile(){

      // Get upload form.
      const form =
        document.getElementById(
          "uploadForm"
        );


      // Convert form contents into FormData.
      const fd =
        new FormData(form);


      // Send POST request to Flask /upload endpoint.
      const res =
        await apiRequest(
          '/upload',
          {
            method:'POST',
            body:fd
          }
        );


      // Convert Flask JSON response into JavaScript object.
      const msg =
        await res.json();


      // Display response to user.
      alert(
        msg.message
      );
    }


    // ------------------------------------------------------
    // Download file from browser.
    // ------------------------------------------------------

    async function downloadFile(){

      // Get download form.
      const form =
        document.getElementById(
          "downloadForm"
        );


      // Convert form into FormData.
      const fd =
        new FormData(form);


      // Send POST request to Flask /download endpoint.
      const res =
        await apiRequest(
          '/download',
          {
            method:'POST',
            body:fd
          }
        );


      // Convert response into JSON.
      const msg =
        await res.json();


      // Display response.
      alert(
        msg.message
      );
    }


    // ------------------------------------------------------
    // Periodically fetch logs and system status.
    // ------------------------------------------------------

    async function fetchLogs(){

      try{

        // Request both endpoints simultaneously.
        //
        // Promise.all() waits for both requests.
        const [
          logsRes,
          statusRes
        ] = await Promise.all([

          fetch('/logs'),

          fetch('/status')

        ]);


        // Convert responses into text.
        const logs =
          await logsRes.text();

        const status =
          await statusRes.text();


        // Find the dashboard log area.
        const logArea =
          document.getElementById(
            'logArea'
          );


        // Display logs + system status.
        logArea.textContent =
          logs +
          "\\n\\n" +
          status;


        // Automatically scroll to latest log.
        logArea.scrollTop =
          logArea.scrollHeight;


      }catch(e){

        // Display error if logs/status cannot be retrieved.
        document.getElementById(
          'logArea'
        ).textContent =
          'Unable to fetch logs/status: ' + e;

      }

    }


    // ------------------------------------------------------
    // Refresh dashboard every 3 seconds.
    // ------------------------------------------------------

    setInterval(
      fetchLogs,
      3000
    );


    // Fetch immediately when page loads.
    fetchLogs();

  </script>

</body>

</html>
"""


# ============================================================
# FLASK ROUTES
# ============================================================
#
# Flask routes map URLs to Python functions.
#
# Example:
#
#       /
#       /upload
#       /download
#       /logs
#       /status
#
# ============================================================


# ------------------------------------------------------------
# Dashboard homepage
# ------------------------------------------------------------

@app.route("/")
def dashboard():

    # Render HTML_PAGE directly.
    return render_template_string(
        HTML_PAGE
    )


# ------------------------------------------------------------
# UPLOAD API
# ------------------------------------------------------------

@app.route(
    "/upload",
    methods=["POST"]
)
def api_upload():

    # Get uploaded file from HTTP request.
    file = request.files["file"]


    # Create local path where the uploaded file will
    # temporarily be stored.
    path = os.path.join(
        ".",
        file.filename
    )


    # Save uploaded file locally.
    file.save(
        path
    )


    # --------------------------------------------------------
    # Start upload in a separate background thread.
    #
    # This prevents Flask from blocking while the file is
    # being split and transferred to DataNodes.
    # --------------------------------------------------------

    Thread(
        target=upload_file,
        args=(path,),
        daemon=True
    ).start()


    # Immediately tell browser that upload started.
    return jsonify(
        {
            "message":
                f"Uploading {file.filename}..."
        }
    )


# ------------------------------------------------------------
# DOWNLOAD API
# ------------------------------------------------------------

@app.route(
    "/download",
    methods=["POST"]
)
def api_download():

    # Get filename submitted by browser.
    filename = request.form.get(
        "filename"
    )


    # Reconstructed file will be stored with this name.
    output_path = (
        f"reconstructed_{filename}"
    )


    # Run download in background thread.
    #
    # This prevents the Flask request from blocking while
    # chunks are being downloaded.
    Thread(
        target=download_file,
        args=(
            filename,
            output_path
        ),
        daemon=True
    ).start()


    # Immediately respond to browser.
    return jsonify(
        {
            "message":
                f"Downloading {filename}..."
        }
    )


# ------------------------------------------------------------
# LOGS API
# ------------------------------------------------------------

@app.route(
    "/logs"
)
def api_logs():

    # Return latest 100 logs.
    return "\n".join(
        LOG_BUFFER[-100:]
    )


# ------------------------------------------------------------
# SYSTEM STATUS API
# ------------------------------------------------------------

@app.route(
    "/status"
)
def api_status():

    # Get current status from NameNode and return it.
    return get_system_status()


# ============================================================
# START FLASK DASHBOARD
# ============================================================

def start_dashboard():

    # Start Flask server on all network interfaces.
    #
    # Port 8080 is used for the web dashboard.
    #
    # debug=False prevents Flask development debugging mode.
    app.run(
        host="0.0.0.0",
        port=8080,
        debug=False
    )


# ============================================================
# MAIN DRIVER
# ============================================================
#
# This is where execution begins when the file is run
# directly.
#
# ============================================================

if __name__ == "__main__":


    # --------------------------------------------------------
    # Start Flask dashboard in a background thread.
    #
    # The dashboard runs independently from the rest of the
    # client logic.
    # --------------------------------------------------------

    Thread(
        target=start_dashboard,
        daemon=True
    ).start()


    # Tell user where the dashboard is available.
    print(
        "🌐 Dashboard running at "
        "http://localhost:8080"
    )


    # --------------------------------------------------------
    # Keep main process alive.
    #
    # Background threads are daemon threads, so without
    # keeping the main thread alive, the program would exit.
    # --------------------------------------------------------

    while True:

        # Sleep to avoid wasting CPU.
        time.sleep(1)
