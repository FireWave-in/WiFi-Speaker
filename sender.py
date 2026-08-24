import asyncio
import threading
from pathlib import Path

import numpy as np
import soundcard as sc

from fastapi import FastAPI, WebSocket
from fastapi.responses import FileResponse

import uvicorn


app = FastAPI()


# ============================================================
# Configuration
# ============================================================

SAMPLE_RATE = 48000
CHANNELS = 2

# 40 ms of audio per packet
BLOCK_SIZE = 1920

BASE_DIR = Path(__file__).resolve().parent
RECEIVER_FILE = BASE_DIR / "receiver" / "index.html"


# ============================================================
# Global state
# ============================================================

clients = set()

audio_queue = None

capture_thread = None

stop_capture = threading.Event()


# ============================================================
# Audio device
# ============================================================

def get_loopback_microphone():
    """
    Get the Windows WASAPI loopback device
    corresponding to the default speaker.
    """

    speaker = sc.default_speaker()

    if speaker is None:
        raise RuntimeError(
            "No default speaker found."
        )

    print(
        f"Using speaker: {speaker.name}"
    )

    microphone = sc.get_microphone(
        id=str(speaker.id),
        include_loopback=True
    )

    if microphone is None:
        raise RuntimeError(
            "Could not access speaker loopback."
        )

    return microphone


# ============================================================
# Audio capture
# ============================================================

def capture_audio(loop):
    """
    Capture Windows system audio in a dedicated thread.

    Running capture separately from asyncio prevents
    network activity from interfering with the audio
    capture loop.
    """

    try:

        microphone = get_loopback_microphone()

        print(
            "Starting system audio capture..."
        )

        with microphone.recorder(
            samplerate=SAMPLE_RATE,
            channels=CHANNELS,
            blocksize=BLOCK_SIZE
        ) as recorder:

            while not stop_capture.is_set():

                audio = recorder.record(
                    numframes=BLOCK_SIZE
                )

                # Keep samples inside [-1, 1].
                audio = np.clip(
                    audio,
                    -1.0,
                    1.0
                )

                # Convert float audio to
                # signed 16-bit PCM.
                pcm = (
                    audio * 32767
                ).astype(np.int16)

                data = pcm.tobytes()

                # Safely pass the audio packet
                # from the capture thread to asyncio.
                try:

                    asyncio.run_coroutine_threadsafe(
                        put_audio(data),
                        loop
                    )

                except RuntimeError:
                    break

    except Exception as e:

        print(
            f"Audio capture error: {e}"
        )


# ============================================================
# Audio queue
# ============================================================

async def put_audio(data: bytes):
    """
    Put captured audio into the asyncio queue.

    If the queue becomes full, discard the oldest
    packet to prevent latency from continuously growing.
    """

    if audio_queue is None:
        return

    if audio_queue.full():

        try:
            audio_queue.get_nowait()

        except asyncio.QueueEmpty:
            pass

    try:

        audio_queue.put_nowait(data)

    except asyncio.QueueFull:
        pass


# ============================================================
# Audio broadcaster
# ============================================================

async def broadcast_audio():
    """
    Take audio packets from the queue and send them
    to every connected phone.
    """

    while True:

        data = await audio_queue.get()

        if not clients:
            continue

        dead_clients = set()

        for client in list(clients):

            try:

                await client.send_bytes(
                    data
                )

            except Exception:

                dead_clients.add(client)

        clients.difference_update(
            dead_clients
        )


# ============================================================
# WebSocket endpoint
# ============================================================

@app.websocket("/audio")
async def audio_socket(
    websocket: WebSocket
):

    await websocket.accept()

    clients.add(websocket)

    address = websocket.client

    print(
        f"Phone connected: {address}"
    )

    try:

        while True:

            # We don't require any messages
            # from the phone.
            #
            # This simply waits until the
            # WebSocket disconnects.

            await websocket.receive()

    except Exception:
        pass

    finally:

        clients.discard(websocket)

        print(
            f"Phone disconnected: {address}"
        )


# ============================================================
# Receiver webpage
# ============================================================

@app.get("/")
async def index():

    if not RECEIVER_FILE.exists():

        return {
            "error": "receiver/index.html not found",
            "expected_path": str(RECEIVER_FILE)
        }

    return FileResponse(
        RECEIVER_FILE,
        media_type="text/html"
    )


# ============================================================
# Server status
# ============================================================

@app.get("/status")
async def status():

    return {
        "name": "Wi-Fi Speaker",
        "status": "running",
        "connected_devices": len(clients),
        "audio": {
            "sample_rate": SAMPLE_RATE,
            "channels": CHANNELS,
            "block_size": BLOCK_SIZE,
            "packet_duration_ms": 40,
            "format": "PCM S16LE"
        }
    }


# ============================================================
# Startup
# ============================================================

@app.on_event("startup")
async def startup():

    global audio_queue
    global capture_thread

    print(
        "Initializing audio system..."
    )

    # Queue holds a small amount of audio.
    #
    # 20 × 40 ms = 800 ms maximum buffering.
    audio_queue = asyncio.Queue(
        maxsize=20
    )

    # Start broadcaster.
    asyncio.create_task(
        broadcast_audio()
    )

    # Get the currently running asyncio loop.
    loop = asyncio.get_running_loop()

    # Start capture in a separate thread.
    capture_thread = threading.Thread(
        target=capture_audio,
        args=(loop,),
        daemon=True
    )

    capture_thread.start()


# ============================================================
# Shutdown
# ============================================================

@app.on_event("shutdown")
async def shutdown():

    print(
        "Stopping audio capture..."
    )

    stop_capture.set()

    if capture_thread is not None:

        capture_thread.join(
            timeout=2
        )

    print(
        "Audio capture stopped."
    )


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    print()
    print("=" * 55)
    print("                 Wi-Fi Speaker")
    print("=" * 55)
    print()

    print(
        "Audio format : 48 kHz / Stereo / 16-bit PCM"
    )

    print(
        "Packet size  : 40 ms"
    )

    print(
        "Transport    : WebSocket"
    )

    print()

    print(
        "Starting server..."
    )

    print(
        "Listening on all network interfaces."
    )

    print()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )