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

# 40 ms per packet
BLOCK_SIZE = 1920

BASE_DIR = Path(__file__).resolve().parent
RECEIVER_FILE = BASE_DIR / "receiver" / "index.html"


# ============================================================
# Global state
# ============================================================

clients = set()

# WebSocket -> device information
devices = {}

audio_queue = None

capture_thread = None

stop_capture = threading.Event()


# ============================================================
# Audio device
# ============================================================

def get_loopback_microphone():

    speaker = sc.default_speaker()

    if speaker is None:
        raise RuntimeError(
            "No default speaker found."
        )

    print(f"Using speaker: {speaker.name}")

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

    try:

        microphone = get_loopback_microphone()

        print("Starting system audio capture...")

        with microphone.recorder(
            samplerate=SAMPLE_RATE,
            channels=CHANNELS,
            blocksize=BLOCK_SIZE
        ) as recorder:

            while not stop_capture.is_set():

                audio = recorder.record(
                    numframes=BLOCK_SIZE
                )

                audio = np.clip(
                    audio,
                    -1.0,
                    1.0
                )

                pcm = (
                    audio * 32767
                ).astype(np.int16)

                data = pcm.tobytes()

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

    while True:

        data = await audio_queue.get()

        if not clients:
            continue

        dead_clients = set()

        for client in list(clients):

            try:

                await client.send_bytes(data)

            except Exception:

                dead_clients.add(client)

        for client in dead_clients:

            clients.discard(client)
            devices.pop(client, None)


# ============================================================
# WebSocket
# ============================================================

@app.websocket("/audio")
async def audio_socket(websocket: WebSocket):

    await websocket.accept()

    clients.add(websocket)

    address = websocket.client

    print(
        f"Device connected: {address}"
    )

    try:

        while True:

            message = await websocket.receive()

            # Device identification message
            if message.get("text"):

                try:

                    import json

                    data = json.loads(
                        message["text"]
                    )

                    if data.get("type") == "identify":

                        devices[websocket] = {
                            "name": data.get(
                                "name",
                                "Unknown Device"
                            ),
                            "platform": data.get(
                                "platform",
                                "Unknown"
                            ),
                            "browser": data.get(
                                "browser",
                                "Unknown"
                            ),
                            "ip": address.host
                            if address else "Unknown"
                        }

                        print(
                            f"  Name     : "
                            f"{devices[websocket]['name']}"
                        )

                        print(
                            f"  Platform : "
                            f"{devices[websocket]['platform']}"
                        )

                        print(
                            f"  Browser  : "
                            f"{devices[websocket]['browser']}"
                        )

                        print(
                            f"  IP       : "
                            f"{devices[websocket]['ip']}"
                        )

                        print()

                except Exception as e:

                    print(
                        f"Device identification error: {e}"
                    )

    except Exception:
        pass

    finally:

        clients.discard(websocket)

        device = devices.pop(
            websocket,
            None
        )

        if device:

            print(
                f"Device disconnected: "
                f"{device['name']}"
            )

        else:

            print(
                f"Device disconnected: {address}"
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
# Device list
# ============================================================

@app.get("/devices")
async def get_devices():

    result = []

    for device in devices.values():

        result.append(device)

    return {
        "count": len(result),
        "devices": result
    }


# ============================================================
# Server status
# ============================================================

@app.get("/status")
async def status():

    return {
        "name": "Wi-Fi Speaker",
        "status": "running",
        "connected_devices": len(clients),
        "devices": list(
            devices.values()
        ),
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

    audio_queue = asyncio.Queue(
        maxsize=20
    )

    asyncio.create_task(
        broadcast_audio()
    )

    loop = asyncio.get_running_loop()

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