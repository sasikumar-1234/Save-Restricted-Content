import sys
import subprocess
import os
import asyncio
import logging
import random
import json
import math
import gc
import time
from datetime import datetime, timezone, timedelta
from google.colab import userdata, drive
import nest_asyncio

print("🔄 Initializing environment and dependencies...")
subprocess.check_call([sys.executable, "-m", "pip", "install", "telethon", "cryptg", "tqdm", "nest_asyncio", "-q"])
subprocess.check_call(["apt-get", "update", "-qq"])
subprocess.check_call(["apt-get", "install", "-y", "ffmpeg", "-qq"])

import cryptg
from telethon import TelegramClient, errors, functions, types, utils
from telethon.sessions import StringSession
from telethon.errors import AuthKeyError, UnauthorizedError
from tqdm.notebook import tqdm

# ==========================================
# 0. SETUP & CREDENTIALS
# ==========================================
nest_asyncio.apply()
drive.mount('/content/drive')

logging.getLogger('telethon').setLevel(logging.ERROR)
logging.getLogger('asyncio').setLevel(logging.CRITICAL)

API_ID =         # Replace with your API_ID
API_HASH = ""     # Replace with your API_HASH
TELETHON_SESSION = userdata.get('TELETHON_SESSION')
BOT_SESSION_STRING = " "

SOURCE_INPUT = "-1002320221806_943"  
DESTINATION_CHAT = -1003846210054    

if "_" in str(SOURCE_INPUT):
    chat_part, topic_part = str(SOURCE_INPUT).split("_")
    SOURCE_CHAT = int(chat_part)
    TOPIC_ID = int(topic_part)
else:
    SOURCE_CHAT = int(SOURCE_INPUT)
    TOPIC_ID = None

# ==========================================
# 1. STATE MANAGEMENT & GLOBALS
# ==========================================
QUEUE_DIR = "/content/drive/MyDrive/Migration_Queue"
os.makedirs(QUEUE_DIR, exist_ok=True)
STATE_FILE = os.path.join(QUEUE_DIR, f"state_{abs(SOURCE_CHAT)}_{TOPIC_ID or 'channel'}.json")

active_temp_files = set()
shutdown_event = asyncio.Event()
last_activity = time.time()
DAILY_LIMIT_GB = 40
DAILY_LIMIT_BYTES = DAILY_LIMIT_GB * 1024 * 1024 * 1024

def update_activity():
    global last_activity
    last_activity = time.time()

def load_state():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    default_state = {"last_id": 0, "daily_bytes": 0, "failed_ids": [], "date": today}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            if state.get("date") != today:
                state["date"] = today
                state["daily_bytes"] = 0
                save_state(state)
            return state
        except Exception:
            return default_state
    return default_state

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def cleanup_temp_files():
    for f in list(active_temp_files):
        try:
            if os.path.exists(f): os.remove(f)
        except Exception: pass
    active_temp_files.clear()

# ==========================================
# 2. ROBUST CLIENT INSTANTIATION
# ==========================================
user_client = TelegramClient(
    StringSession(TELETHON_SESSION), API_ID, API_HASH,
    connection_retries=None, request_retries=10, flood_sleep_threshold=120
)
bot_client = TelegramClient(
    StringSession(BOT_SESSION_STRING), API_ID, API_HASH,
    connection_retries=None, request_retries=10, flood_sleep_threshold=120
)

# ==========================================
# 3. STREAM PROBING & FFMPEG SLICING
# ==========================================
def probe_media(file_path):
    cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height,duration:format=duration", "-of", "json", file_path]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    duration, width, height = 0, 1280, 720
    try:
        data = json.loads(result.stdout)
        if "streams" in data and len(data["streams"]) > 0:
            s = data["streams"][0]
            width, height = int(s.get("width", 1280)), int(s.get("height", 720))
            if "duration" in s: duration = float(s["duration"])
        if duration == 0 and "format" in data and "duration" in data["format"]:
            duration = float(data["format"]["duration"])
    except Exception: pass
    return duration, width, height

async def process_large_video(file_path):
    file_size = os.path.getsize(file_path)
    limit = 1.95 * 1024 * 1024 * 1024
    if file_size <= limit:
        dur, w, h = probe_media(file_path)
        return [(file_path, dur, w, h)]

    dur, w, h = probe_media(file_path)
    if dur == 0: return [(file_path, 0, w, h)]

    parts_needed = math.ceil(file_size / limit)
    segment_time = math.ceil(dur / parts_needed)
    base_name, ext = os.path.splitext(file_path)
    output_pattern = f"{base_name}_part%03d{ext}"

    subprocess.run([
        "ffmpeg", "-i", file_path, "-c", "copy", "-map", "0",
        "-segment_time", str(segment_time), "-f", "segment",
        "-reset_timestamps", "1", output_pattern, "-y"
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if file_path in active_temp_files: active_temp_files.remove(file_path)
    os.remove(file_path)

    split_paths = sorted([os.path.join(os.path.dirname(file_path), f)
                          for f in os.listdir(os.path.dirname(file_path))
                          if f.startswith(os.path.basename(base_name) + "_part")])

    processed_segments = []
    for p in split_paths:
        active_temp_files.add(p)
        s_dur, s_w, s_h = probe_media(p)
        processed_segments.append((p, s_dur, s_w or w, s_h or h))
    return processed_segments

# ==========================================
# 4. NATIVE, DOCUMENTATION-COMPLIANT I/O ENGINE
# ==========================================
async def native_download(client, msg, file_path):
    file_size = getattr(msg.document, 'size', getattr(msg.video, 'size', 0)) if hasattr(msg, 'document') or hasattr(msg, 'video') else 0
    if file_size == 0 and not getattr(msg, 'photo', None): return None 
    
    filename = os.path.basename(file_path)
    print(f"\n[BUFFER DOWNLOAD] ID {msg.id} | {filename}")

    desc_text = f"📥 DL: {filename[:15]}..."
    
    with tqdm(total=file_size, desc=desc_text, unit='B', unit_scale=True, unit_divisor=1024, colour="blue", position=0, leave=False, dynamic_ncols=True) as pbar:
        async def progress_callback(current, total):
            pbar.n = current
            pbar.update(0)
            update_activity()

        for attempt in range(5):
            try:
                # Telethon natively handles chunk sizing and MTProto pacing with cryptg speed
                await client.download_media(msg, file=file_path, progress_callback=progress_callback)
                return file_path
            except Exception as e:
                if attempt == 4: raise e
                print(f"\n⚠️ DL Interrupted ({e}). Retrying ({attempt+1}/5)...")
                if os.path.exists(file_path): os.remove(file_path)
                await asyncio.sleep(2.0 ** attempt + random.uniform(0.1, 0.5))

async def native_upload(client, file_path):
    file_size = os.path.getsize(file_path)
    filename = os.path.basename(file_path)
    desc_text = f"📤 UP: {filename[:15]}..."

    with tqdm(total=file_size, desc=desc_text, unit='B', unit_scale=True, unit_divisor=1024, colour="green", position=1, leave=False, dynamic_ncols=True) as pbar:
        async def progress_callback(current, total):
            pbar.n = current
            pbar.update(0)
            update_activity()

        for attempt in range(5):
            try:
                # Natively handles SaveBigFilePart concurrency safely
                uploaded_file = await client.upload_file(file_path, part_size_kb=512, progress_callback=progress_callback)
                return uploaded_file
            except Exception as e:
                if attempt == 4: raise e
                print(f"\n⚠️ UP Interrupted ({e}). Retrying ({attempt+1}/5)...")
                await asyncio.sleep(2.0 ** attempt + random.uniform(0.1, 0.5))

# ==========================================
# 5. PIPELINE & COORDINATION
# ==========================================
async def prepare_bundle_data(msgs):
    prepared_items = []
    for msg in msgs:
        if getattr(msg, 'action', None): continue
        if not getattr(msg, 'media', None):
            prepared_items.append({"msg": msg, "is_text": True})
            continue

        file_title = getattr(getattr(msg, 'file', None), 'name', None)
        if not file_title and hasattr(msg, 'document') and msg.document:
            for attr in msg.document.attributes:
                if hasattr(attr, 'title') and attr.title: file_title = attr.title; break
                elif hasattr(attr, 'file_name') and attr.file_name: file_title = attr.file_name; break

        ext = utils.get_extension(msg.media) or '.mp4'
        file_title_fallback = file_title or f"media_{msg.id}{ext}"
        current_file_path = os.path.join(os.getcwd(), file_title_fallback)
        active_temp_files.add(current_file_path)

        thumb_path = None
        if hasattr(msg, 'document') and msg.document and getattr(msg.document, 'thumbs', None):
            thumb_path = current_file_path + "_thumb.jpg"
            try:
                await user_client.download_media(msg, file=thumb_path, thumb=-1)
                if os.path.exists(thumb_path): active_temp_files.add(thumb_path)
                else: thumb_path = None
            except Exception: thumb_path = None

        # Replaced custom downloader with the reliable native downloader
        current_file_path = await native_download(user_client, msg, current_file_path)
        
        if current_file_path and os.path.exists(current_file_path):
            segments = await process_large_video(current_file_path)
            prepared_items.append({"msg": msg, "is_text": False, "segments": segments, "thumb_path": thumb_path})

    return {"msgs": msgs, "items": prepared_items}

async def producer(queue, state):
    quota_hit = False
    last_id = state["last_id"]
    buffer = []
    current_group = None
    kwargs = {'reverse': True, 'min_id': last_id}
    if TOPIC_ID: kwargs['reply_to'] = TOPIC_ID

    async for msg in user_client.iter_messages(SOURCE_CHAT, **kwargs):
        if shutdown_event.is_set(): break
        if msg.id == TOPIC_ID or msg.id in state.get("failed_ids", []): continue
        if (msg.sticker or msg.gif or msg.audio or msg.voice or msg.video_note or msg.poll or msg.dice or msg.contact or msg.geo or msg.game): continue
        
        if state["daily_bytes"] >= DAILY_LIMIT_BYTES:
            print(f"\n🛑 Daily quota reached ({DAILY_LIMIT_GB} GB). Downloader pausing for today.")
            quota_hit = True
            break

        if msg.grouped_id:
            if current_group == msg.grouped_id: buffer.append(msg)
            else:
                if buffer:
                    pb = await prepare_bundle_data(buffer)
                    if pb: await queue.put(pb)
                buffer = [msg]
                current_group = msg.grouped_id
        else:
            if buffer:
                pb = await prepare_bundle_data(buffer)
                if pb: await queue.put(pb)
                buffer = []
                current_group = None
            pb = await prepare_bundle_data([msg])
            if pb: await queue.put(pb)

    if buffer and not shutdown_event.is_set() and not quota_hit:
        pb = await prepare_bundle_data(buffer)
        if pb: await queue.put(pb)

    await queue.put(None)
    return quota_hit

async def consumer(queue, state):
    items_count = 0 
    while not shutdown_event.is_set():
        data = await queue.get()
        if data is None:
            queue.task_done()
            break

        msgs, items = data["msgs"], data["items"]
        uploaded_media, captions, original_attributes, thumb_paths = [], [], [], []
        total_bundle_bytes = 0

        try:
            for item in items:
                msg = item["msg"]
                if item["is_text"]:
                    safe_text = (msg.text[:1020] + "...") if msg.text and len(msg.text) > 1024 else (msg.text or "")
                    await bot_client.send_message(DESTINATION_CHAT, safe_text, formatting_entities=msg.entities)
                    continue

                for idx, (seg_file, dur, w, h) in enumerate(item["segments"]):
                    f_size = os.path.getsize(seg_file)
                    
                    # Replaced custom uploader with the reliable native uploader
                    uploaded_file = await native_upload(bot_client, seg_file)
                    uploaded_media.append(uploaded_file)
                    
                    raw_caption = msg.text if idx == 0 else ""
                    safe_caption = (raw_caption[:1020] + "...") if raw_caption and len(raw_caption) > 1024 else raw_caption
                    captions.append(safe_caption)
                    
                    thumb_paths.append(item["thumb_path"])
                    total_bundle_bytes += f_size
                    original_attributes.append([types.DocumentAttributeVideo(duration=int(dur), w=w, h=h, supports_streaming=True)])
                    if seg_file in active_temp_files: active_temp_files.remove(seg_file)
                    os.remove(seg_file)

            if uploaded_media:
                if len(uploaded_media) == 1:
                    await bot_client.send_file(
                        DESTINATION_CHAT, file=uploaded_media[0], caption=captions[0],
                        formatting_entities=msgs[0].entities, attributes=original_attributes[0],
                        supports_streaming=True, thumb=thumb_paths[0]
                    )
                else:
                    await bot_client.send_file(
                        DESTINATION_CHAT, file=uploaded_media, caption=captions,
                        supports_streaming=True, thumb=thumb_paths
                    )
                update_activity()

            for item in items:
                tp = item.get("thumb_path")
                if tp and os.path.exists(tp):
                    if tp in active_temp_files: active_temp_files.remove(tp)
                    os.remove(tp)

            state["last_id"] = msgs[-1].id
            state["daily_bytes"] += total_bundle_bytes
            save_state(state)
            items_count += len(msgs)
            gc.collect()

            log_id = msgs[0].id if len(msgs) == 1 else f"{msgs[0].id}-{msgs[-1].id}"
            print(f"✅ [COMMITTED] ID {log_id} | Session Total: {state['daily_bytes'] / (1024**3):.2f} GB")
            
            # 15-File Milestone Break Preserved
            if items_count > 0 and items_count % 15 == 0:
                rest = random.randint(30, 45)
                print(f"☕ Milestone break ({rest}s)...")
                await asyncio.sleep(rest)
            
            # Standard Per-File Delay Preserved
            await asyncio.sleep(random.uniform(2.5, 5.5))

        except Exception as e:
            print(f"❌ Bundle failure at ID {msgs[0].id}: {e}")
            state["failed_ids"].append(msgs[0].id)
            save_state(state)
            for item in items:
                for seg_file, _, _, _ in item.get("segments", []):
                    if os.path.exists(seg_file): 
                        try: os.remove(seg_file)
                        except: pass
        finally:
            queue.task_done()

# ==========================================
# 6. WATCHDOG & EXECUTION MANAGER
# ==========================================
async def run_clone():
    print("🚀 Connecting clients...")
    await user_client.start()
    await bot_client.start()

    print("🛂 Pre-Flight Health Checks...")
    try:
        test = await bot_client.send_message(DESTINATION_CHAT, "🔄 Startup bot permission test...")
        await test.delete()
    except Exception as e:
        print(f"❌ FATAL: Bot lacks write permissions in DESTINATION_CHAT: {e}")
        return "FATAL"

    state = load_state()
    print(f"📂 State loaded. Resuming at ID: {state['last_id']}")
    update_activity()

    async def watchdog():
        while not shutdown_event.is_set():
            await asyncio.sleep(30)
            if time.time() - last_activity > 600: 
                print("\n⚠️ Watchdog timeout: Pipeline frozen for 10 minutes. Forcing socket reset.")
                shutdown_event.set()
                await user_client.disconnect()
                await bot_client.disconnect()
                break

    watchdog_task = asyncio.create_task(watchdog())
    queue = asyncio.Queue(maxsize=1)
    
    prod_task = asyncio.create_task(producer(queue, state))
    cons_task = asyncio.create_task(consumer(queue, state))

    try:
        quota_hit = await prod_task
        await cons_task
    except asyncio.CancelledError:
        shutdown_event.set()
        quota_hit = False
    finally:
        watchdog_task.cancel()

    await user_client.disconnect()
    await bot_client.disconnect()
    cleanup_temp_files()
    
    return "QUOTA" if quota_hit else "DONE"

async def master_loop():
    while True:
        shutdown_event.clear()
        try:
            status = await run_clone()
            if status == "FATAL":
                break
            elif status == "DONE":
                print("🎉 Entire migration completed successfully.")
                break
            elif status == "QUOTA":
                now = datetime.now(timezone.utc)
                tomorrow = now + timedelta(days=1)
                midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, tzinfo=timezone.utc)
                sleep_seconds = (midnight - now).total_seconds() + 120 
                print(f"💤 Sleeping for {sleep_seconds/3600:.2f} hours until UTC midnight rollover...")
                await asyncio.sleep(sleep_seconds)
                print("🌅 New day started. Waking up and resuming...")
                
        except (AuthKeyError, UnauthorizedError) as e:
            print(f"\n🛑 FATAL AUTH ERROR: {e}. Session revoked or banned. Stopping script.")
            break
        except Exception as e:
            print(f"\n🔄 Outer loop caught an error ({e}). Restarting pipeline in 15 seconds...")
            cleanup_temp_files()
            await asyncio.sleep(15)

try:
    await master_loop()
except KeyboardInterrupt:
    shutdown_event.set()
    cleanup_temp_files()
    print("\n🛑 Script terminated safely via Keyboard Interrupt.")
