import asyncio
import json
import threading
from pathlib import Path

import numpy as np
import soundcard as sc

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import uvicorn


app = FastAPI()


# ============================================================
# Configuration
# ============================================================

SAMPLE_RATE = 48000
CHANNELS = 2

# 40 ms
BLOCK_SIZE = 1920

BASE_DIR = Path(__file__).resolve().parent

RECEIVER_FILE = (
    BASE_DIR / "receiver" / "index.html"
)

CONTROL_FILE = (
    BASE_DIR / "control" / "index.html"
)
app.mount(
    "/receiver",
    StaticFiles(directory=BASE_DIR / "receiver"),
    name="receiver"
)

# ============================================================
# Global state
# ============================================================

clients = set()

devices = {}

audio_queue = None

capture_thread = None

stop_capture = threading.Event()


# ============================================================
# Device helpers
# ============================================================

def device_info(websocket):

    device = devices.get(websocket)

    if device is None:
        return None

    return device


def selected_clients():

    return [
        client
        for client in clients
        if client in devices
        and devices[client]["selected"]
    ]


# ============================================================
# Audio device
# ============================================================

def get_loopback_microphone():

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

    try:

        microphone = (
            get_loopback_microphone()
        )

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

async def put_audio(data):

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

        targets = selected_clients()

        if not targets:
            continue

        dead_clients = set()

        for client in targets:

            try:

                await client.send_bytes(
                    data
                )

            except Exception:

                dead_clients.add(client)

        for client in dead_clients:

            clients.discard(client)
            devices.pop(client, None)


# ============================================================
# Receiver WebSocket
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

            if message.get("text"):

                try:

                    data = json.loads(
                        message["text"]
                    )

                    if (
                        data.get("type")
                        == "identify"
                    ):

                        name = data.get(
                            "name",
                            "Unknown Speaker"
                        )

                        platform = data.get(
                            "platform",
                            "Unknown"
                        )

                        browser = data.get(
                            "browser",
                            "Unknown"
                        )

                        devices[websocket] = {

                            "name": name,

                            "platform":
                                platform,

                            "browser":
                                browser,

                            "ip":
                                address.host
                                if address
                                else "Unknown",

                            "selected":
                                True,

                            "volume":
                                1.0
                        }

                        print()
                        print(
                            "Speaker identified:"
                        )

                        print(
                            f"  Name     : {name}"
                        )

                        print(
                            f"  Platform : {platform}"
                        )

                        print(
                            f"  Browser  : {browser}"
                        )

                        print(
                            f"  IP       : "
                            f"{address.host}"
                        )

                        print()

                except Exception as e:

                    print(
                        "Identification error:",
                        e
                    )

    except WebSocketDisconnect:

        pass

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
                f"Speaker disconnected: "
                f"{device['name']}"
            )

        else:

            print(
                f"Device disconnected: "
                f"{address}"
            )


# ============================================================
# Control WebSocket
# ============================================================

@app.websocket("/control/ws")
async def control_socket(websocket: WebSocket):

    await websocket.accept()

    print(
        "Control panel connected."
    )

    try:

        while True:

            message = await websocket.receive_text()

            data = json.loads(message)

            command = data.get("command")

            # --------------------------------------------
            # Select device
            # --------------------------------------------

            if command == "select":

                index = data.get("index")

                for client, device in devices.items():

                    if id(client) == index:

                        device["selected"] = True

            # --------------------------------------------
            # Deselect device
            # --------------------------------------------

            elif command == "deselect":

                index = data.get("index")

                for client, device in devices.items():

                    if id(client) == index:

                        device["selected"] = False

            # --------------------------------------------
            # Select all
            # --------------------------------------------

            elif command == "select_all":

                for device in devices.values():

                    device["selected"] = True

            # --------------------------------------------
            # Deselect all
            # --------------------------------------------

            elif command == "deselect_all":

                for device in devices.values():

                    device["selected"] = False

            # --------------------------------------------
            # Volume
            # --------------------------------------------

            elif command == "volume":

                index = data.get("index")

                volume = float(
                    data.get(
                        "volume",
                        1.0
                    )
                )

                volume = max(
                    0.0,
                    min(1.0, volume)
                )

                for client, device in devices.items():

                    if id(client) == index:

                        device["volume"] = volume

                        try:

                            await client.send_text(
                                json.dumps({
                                    "type":
                                        "volume",

                                    "volume":
                                        volume
                                })
                            )

                        except Exception:

                            pass

            # --------------------------------------------
            # Mute
            # --------------------------------------------

            elif command == "mute":

                index = data.get("index")

                for client, device in devices.items():

                    if id(client) == index:

                        device["muted"] = not device.get(
                            "muted",
                            False
                        )

                        try:

                            await client.send_text(
                                json.dumps({
                                    "type":
                                        "mute",

                                    "muted":
                                        device["muted"]
                                })
                            )

                        except Exception:

                            pass

    except Exception:

        pass

    finally:

        print(
            "Control panel disconnected."
        )


# ============================================================
# Receiver page
# ============================================================

@app.get("/")
async def receiver():

    return FileResponse(
        RECEIVER_FILE,
        media_type="text/html"
    )


# ============================================================
# Control panel
# ============================================================

@app.get("/control")
async def control():

    return FileResponse(
        CONTROL_FILE,
        media_type="text/html"
    )


# ============================================================
# Devices API
# ============================================================

@app.get("/devices")
async def get_devices():

    result = []

    for client, device in devices.items():

        item = dict(device)

        # Temporary identifier used
        # by the control panel.

        item["id"] = id(client)

        result.append(item)

    return {
        "count": len(result),
        "devices": result
    }


# ============================================================
# Status
# ============================================================

@app.get("/status")
async def status():

    return {

        "name":
            "Wi-Fi Speaker",

        "status":
            "running",

        "connected_devices":
            len(clients),

        "selected_devices":
            len(
                selected_clients()
            ),

        "audio": {

            "sample_rate":
                SAMPLE_RATE,

            "channels":
                CHANNELS,

            "block_size":
                BLOCK_SIZE,

            "packet_duration_ms":
                40,

            "format":
                "PCM S16LE"
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
    print("=" * 60)
    print("                 Wi-Fi Speaker")
    print("=" * 60)
    print()

    print(
        "Audio      : 48 kHz / Stereo / 16-bit"
    )

    print(
        "Packet     : 40 ms"
    )

    print(
        "Transport  : WebSocket"
    )

    print()
    print(
        "Receiver   : http://<laptop-ip>:8000"
    )

    print(
        "Control    : http://<laptop-ip>:8000/control"
    )

    print()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )