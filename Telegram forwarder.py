import sys
import subprocess
import os
import asyncio
import logging
import random
import json
import math
from google.colab import userdata, drive
import nest_asyncio

print("🔄 Step 1: Installing Python dependencies and FFmpeg for video processing...")
subprocess.check_call([sys.executable, "-m", "pip", "install", "telethon", "cryptg", "tqdm", "nest_asyncio", "-q"])
subprocess.check_call(["apt-get", "update", "-qq"])
subprocess.check_call(["apt-get", "install", "-y", "ffmpeg", "-qq"])

import cryptg
from telethon import TelegramClient, errors, functions, types, utils
from telethon.sessions import StringSession, MemorySession
from tqdm.asyncio import tqdm

# ==========================================
# 0. SETUP, MOUNT & CREDENTIALS
# ==========================================
nest_asyncio.apply()
drive.mount('/content/drive')

logging.getLogger('telethon').setLevel(logging.ERROR)
logging.getLogger('asyncio').setLevel(logging.CRITICAL)

API_ID =         # Replace with your API_ID
API_HASH = "  "     # Replace with your API_HASH
TELETHON_SESSION = userdata.get('TELETHON_SESSION')
BOT_TOKEN = " "    # Replace with your Bot API Token from @BotFather

# --- SOURCE & DESTINATION CONFIGURATION ---
SOURCE_INPUT = "-1002320221806_943"  # Source (Forum Topic)
DESTINATION_CHAT = -1003846210054    # Destination (Plain Private Channel)

if "_" in str(SOURCE_INPUT):
    chat_part, topic_part = str(SOURCE_INPUT).split("_")
    SOURCE_CHAT = int(chat_part)
    TOPIC_ID = int(topic_part)
else:
    SOURCE_CHAT = int(SOURCE_INPUT)
    TOPIC_ID = None
# ------------------------------------------

# --- JSON STATE QUEUE ---
QUEUE_DIR = "/content/drive/MyDrive/Migration_Queue"
os.makedirs(QUEUE_DIR, exist_ok=True)
STATE_FILE = os.path.join(QUEUE_DIR, f"state_{abs(SOURCE_CHAT)}_{TOPIC_ID or 'channel'}.json")

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"last_id": 0, "daily_bytes": 0, "failed_ids": []}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

# --- DUAL-SESSION INITIALIZATION ---
user_client = TelegramClient(StringSession(TELETHON_SESSION), API_ID, API_HASH)
bot_client = TelegramClient('MemorySession()', API_ID, API_HASH)

# ==========================================
# 1. FFmpeg SEGMENTER & FAST DOWNLOADER
# ==========================================
def get_video_duration(file_path):
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file_path],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    try: return float(result.stdout.strip())
    except ValueError: return 0

async def process_large_video(file_path):
    file_size = os.path.getsize(file_path)
    limit = 1.95 * 1024 * 1024 * 1024  # 1.95GB Threshold to stay safely under 2GB

    if file_size <= limit:
        return [file_path] # No slicing needed

    print(f"\n✂️ [FFMPEG] File exceeds 1.95GB ({file_size/(1024**3):.2f} GB). Slicing via -c copy...")
    duration = get_video_duration(file_path)
    if duration == 0:
        print("⚠️ Could not read duration, attempting raw upload (may fail).")
        return [file_path]

    parts_needed = math.ceil(file_size / limit)
    segment_time = math.ceil(duration / parts_needed)

    base_name, ext = os.path.splitext(file_path)
    output_pattern = f"{base_name}_part%03d{ext}"

    # Execute zero-encoding stream copy
    subprocess.run([
        "ffmpeg", "-i", file_path, "-c", "copy", "-map", "0",
        "-segment_time", str(segment_time), "-f", "segment",
        "-reset_timestamps", "1", output_pattern, "-y"
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    os.remove(file_path) # Delete massive original to save Colab disk space

    split_files = sorted([os.path.join(os.path.dirname(file_path), f)
                          for f in os.listdir(os.path.dirname(file_path))
                          if f.startswith(os.path.basename(base_name) + "_part")])

    print(f"✅ [FFMPEG] Sliced into {len(split_files)} playable segments.")
    return split_files

async def fast_download(client, msg, file_path):
    if not getattr(msg, 'document', None) and not getattr(msg, 'photo', None) and not getattr(msg, 'video', None):
        return await client.download_media(msg, file=file_path)

    file_size = getattr(msg.document, 'size', getattr(msg.video, 'size', 0)) if hasattr(msg, 'document') or hasattr(msg, 'video') else 0

    if getattr(msg, 'photo', None) or file_size < 5 * 1024 * 1024:
        return await client.download_media(msg, file=file_path)

    WORKERS = 8
    chunk_size = 512 * 1024
    total_chunks = (file_size + chunk_size - 1) // chunk_size

    if os.path.exists(file_path): os.remove(file_path)

    with open(file_path, "wb") as f:
        f.truncate(file_size)

    with tqdm(total=file_size, desc="📥 User Reading", unit='B', unit_scale=True, unit_divisor=1024, colour="blue") as pbar:
        async def download_chunk(worker_id, chunk_index):
            offset = chunk_index * chunk_size
            limit = min(chunk_size, file_size - offset)
            for attempt in range(5):
                try:
                    async for chunk in client.iter_download(msg.media, offset=offset, limit=limit, chunk_size=chunk_size):
                        with open(file_path, "r+b") as f:
                            f.seek(offset)
                            f.write(chunk)
                        pbar.update(len(chunk))
                        break
                    break
                except errors.FloodWaitError as e:
                    if attempt == 4: raise e
                    await asyncio.sleep(e.seconds + 1)
                except Exception as e:
                    if attempt == 4: raise e
                    await asyncio.sleep(2)

        queue = asyncio.Queue()
        for i in range(total_chunks): queue.put_nowait(i)

        async def worker(worker_id):
            while not queue.empty():
                try: chunk_index = queue.get_nowait()
                except asyncio.QueueEmpty: break
                await download_chunk(worker_id, chunk_index)
                queue.task_done()

        tasks = [asyncio.create_task(worker(i)) for i in range(WORKERS)]
        await asyncio.gather(*tasks)

    return file_path

# ==========================================
# 2. PARALLEL UPLOADER (BOT CLIENT)
# ==========================================
async def safe_parallel_upload(client, file_path, workers=4):
    file_size = os.path.getsize(file_path)
    if file_size < 15 * 1024 * 1024:
        return await client.upload_file(file_path)

    chunk_size = 512 * 1024
    total_parts = (file_size + chunk_size - 1) // chunk_size
    file_id = int.from_bytes(os.urandom(8), byteorder='little', signed=True)

    semaphore = asyncio.Semaphore(workers)
    uploaded_bytes = [0]

    async def upload_part(part_index):
        for attempt in range(5):
            try:
                async with semaphore:
                    with open(file_path, 'rb') as f:
                        f.seek(part_index * chunk_size)
                        chunk_data = f.read(chunk_size)

                    await client(functions.upload.SaveBigFilePartRequest(
                        file_id=file_id, file_part=part_index,
                        file_total_parts=total_parts, bytes=chunk_data
                    ))
                    uploaded_bytes[0] += len(chunk_data)
                    return True
            except errors.FloodWaitError as e:
                if attempt == 4: raise e
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                if attempt == 4: raise e
                await asyncio.sleep(1.5 ** attempt)

    tasks = [upload_part(i) for i in range(total_parts)]

    with tqdm(total=file_size, desc="📤 Bot Writing", unit='B', unit_scale=True, colour="green") as pbar:
        async def update_bar():
            last_val = 0
            while uploaded_bytes[0] < file_size:
                pbar.update(uploaded_bytes[0] - last_val)
                last_val = uploaded_bytes[0]
                await asyncio.sleep(0.5)
            pbar.update(uploaded_bytes[0] - last_val)

        bar_task = asyncio.create_task(update_bar())
        try: await asyncio.gather(*tasks)
        finally: bar_task.cancel()

    return types.InputFileBig(id=file_id, parts=total_parts, name=os.path.basename(file_path))

# ==========================================
# 3. 1:1 ALBUM-AWARE TRANSFER ENGINE
# ==========================================
async def transfer_bundle(msgs, state):
    uploaded_media = []
    captions = []
    original_attributes = []
    thumb_paths = []
    current_file_path = None
    bundle_bytes_added = 0

    try:
        for msg in msgs:
            if getattr(msg, 'action', None):
                continue

            if not getattr(msg, 'media', None):
                for send_attempt in range(5):
                    try:
                        # USING BOT CLIENT for text
                        await bot_client.send_message(DESTINATION_CHAT, msg.text, formatting_entities=msg.entities)
                        print(f"✅ [BOT] Transferred Text {msg.id}")
                        break
                    except errors.FloodWaitError as e:
                        await asyncio.sleep(e.seconds + 1)
                    except Exception as e:
                        if send_attempt == 4: raise e
                        await asyncio.sleep(2)
                continue

            file_title = getattr(getattr(msg, 'file', None), 'name', None)
            if not file_title and hasattr(msg, 'document') and msg.document:
                for attr in msg.document.attributes:
                    if hasattr(attr, 'title') and attr.title: file_title = attr.title; break
                    elif hasattr(attr, 'file_name') and attr.file_name: file_title = attr.file_name; break

            ext = utils.get_extension(msg.media) or '.mp4'
            file_title_fallback = file_title or f"media_{msg.id}{ext}"
            current_file_path = os.path.join(os.getcwd(), file_title_fallback)

            # --- EXTRACT THUMBNAIL (Via User) ---
            thumb_path = None
            if hasattr(msg, 'document') and msg.document and getattr(msg.document, 'thumbs', None):
                thumb_path = current_file_path + "_thumb.jpg"
                try:
                    await user_client.download_media(msg, file=thumb_path, thumb=-1)
                    if not os.path.exists(thumb_path): thumb_path = None
                except Exception:
                    thumb_path = None
            # -------------------------

            attr = msg.document.attributes if getattr(msg, 'document', None) else None

            log_preview = (msg.text.strip().split('\n')[0][:40] + "...") if msg.text else file_title_fallback
            lbl = "ALBUM PART" if len(msgs) > 1 else "SINGLE"
            print(f"\n[{lbl}] ID: {msg.id} | \"{log_preview}\"")

            for dl_attempt in range(3):
                try:
                    # USING USER CLIENT for reading
                    current_file_path = await fast_download(user_client, msg, current_file_path)
                    if current_file_path and os.path.exists(current_file_path): break
                except Exception as e:
                    print(f"⚠️ DL drop (Attempt {dl_attempt + 1}/3): {e}. Retrying...")
                    await asyncio.sleep(5)

            if current_file_path and os.path.exists(current_file_path):
                # Check size and slice if necessary (O(N) segmentation)
                segmented_files = await process_large_video(current_file_path)

                for index, seg_file in enumerate(segmented_files):
                    file_size = os.path.getsize(seg_file)

                    # USING BOT CLIENT for writing/uploading
                    uploaded_file = await safe_parallel_upload(bot_client, seg_file, workers=8)

                    uploaded_media.append(uploaded_file)
                    original_attributes.append(attr)
                    thumb_paths.append(thumb_path)

                    # Apply caption only to the first part if the video was sliced
                    captions.append(msg.text if index == 0 else "")
                    bundle_bytes_added += file_size

                    os.remove(seg_file) # Immediate cleanup

                current_file_path = None

        if uploaded_media:
            for send_attempt in range(5):
                try:
                    # USING BOT CLIENT to push the final bundle
                    if len(uploaded_media) == 1:
                        await bot_client.send_file(
                            DESTINATION_CHAT,
                            file=uploaded_media[0],
                            caption=captions[0],
                            formatting_entities=msgs[0].entities,
                            attributes=original_attributes[0],
                            supports_streaming=True,
                            thumb=thumb_paths[0]
                        )
                    else:
                        await bot_client.send_file(
                            DESTINATION_CHAT,
                            file=uploaded_media,
                            caption=captions,
                            supports_streaming=True,
                            thumb=thumb_paths
                        )

                    log_id = msgs[0].id if len(msgs) == 1 else f"{msgs[0].id}-{msgs[-1].id}"
                    print(f"✅ [BOT] Successfully Committed Media {log_id}")
                    break
                except errors.FloodWaitError as e:
                    print(f"⏳ [BOT RATE LIMIT] Pausing for {e.seconds}s...")
                    await asyncio.sleep(e.seconds + 1)
                except Exception as e:
                    if send_attempt == 4: raise e
                    await asyncio.sleep(3)

        return True, bundle_bytes_added

    except Exception as e:
        print(f"\n❌ [FATAL ERROR] Bundle failed at ID {msgs[0].id}: {e}")
        return False, 0

    finally:
        if current_file_path and os.path.exists(current_file_path):
            os.remove(current_file_path)
        for t in thumb_paths:
            if t and os.path.exists(t):
                os.remove(t)

# ==========================================
# 4. EXECUTION PIPELINE (JSON QUEUE & SAFETY LOGS)
# ==========================================
async def run_clone():
    print("🚀 Authenticating Dual-Session...")
    await user_client.start()
    await bot_client.start(bot_token=BOT_TOKEN)

    state = load_state()
    last_id = state["last_id"]
    print(f"📂 JSON State Loaded. Resuming from ID {last_id}")

    items_count = 0
    DAILY_LIMIT_GB = 40
    DAILY_LIMIT_BYTES = DAILY_LIMIT_GB * 1024 * 1024 * 1024

    buffer = []
    current_group = None
    kwargs = {'reverse': True, 'min_id': last_id}
    if TOPIC_ID: kwargs['reply_to'] = TOPIC_ID

    print(f"🛡️ [ANTI-BAN ACTIVE] Bot Uploads & Jitter pacing enabled.\n")

    # READING VIA USER
    async for msg in user_client.iter_messages(SOURCE_CHAT, **kwargs):
        if msg.id == TOPIC_ID: continue

        # Skip permanently failed items to prevent infinite loops
        if msg.id in state.get("failed_ids", []):
            print(f"⏩ [SKIPPED] Ignoring previously failed ID {msg.id}")
            continue

        # --- CLUTTER FILTER ---
        if (msg.sticker or msg.gif or msg.audio or msg.voice or
            msg.video_note or msg.poll or msg.dice or
            msg.contact or msg.geo or msg.game):
            print(f"⏩ [SKIPPED] Clutter/unwanted media type at ID {msg.id}")
            continue
        # ----------------------

        # --- SAFETY CIRCUIT BREAKER ---
        if state["daily_bytes"] >= DAILY_LIMIT_BYTES:
            print(f"\n🛑 [SAFETY LOCK TRIGGERED] Daily volume limit of {DAILY_LIMIT_GB} GB reached.")
            print(f"📊 Total pushed today: {state['daily_bytes'] / (1024**3):.2f} GB.")
            break

        if msg.grouped_id:
            if current_group == msg.grouped_id:
                buffer.append(msg)
            else:
                if buffer:
                    success, bytes_added = await transfer_bundle(buffer, state)
                    if not success:
                        state["failed_ids"].append(buffer[0].id)
                        save_state(state)
                        break
                    state["last_id"] = buffer[-1].id
                    state["daily_bytes"] += bytes_added
                    save_state(state)
                    items_count += len(buffer)
                    print(f"📊 [JSON SAVED] Items: {items_count} | Vol: {state['daily_bytes'] / (1024**3):.2f} GB")
                buffer = [msg]
                current_group = msg.grouped_id
        else:
            if buffer:
                success, bytes_added = await transfer_bundle(buffer, state)
                if not success:
                    state["failed_ids"].append(buffer[0].id)
                    save_state(state)
                    break
                state["last_id"] = buffer[-1].id
                state["daily_bytes"] += bytes_added
                save_state(state)
                items_count += len(buffer)
                buffer = []
                current_group = None

            success, bytes_added = await transfer_bundle([msg], state)
            if not success:
                if "failed_ids" not in state: state["failed_ids"] = []
                state["failed_ids"].append(msg.id)
                save_state(state)
                break
            state["last_id"] = msg.id
            state["daily_bytes"] += bytes_added
            save_state(state)
            items_count += 1
            print(f"📊 [JSON SAVED] Items: {items_count} | Vol: {state['daily_bytes'] / (1024**3):.2f} GB")

        # --- DEFENSE 1: MILESTONE PAUSES ---
        if items_count > 0 and items_count % 15 == 0:
            pause_time = random.randint(30, 45)
            print(f"\n☕ [MILESTONE] Resting for {pause_time}s to clear API queue...")
            await asyncio.sleep(pause_time)

        # --- DEFENSE 2: JITTER ---
        await asyncio.sleep(random.uniform(2.5, 5.5))

    # Catch remaining buffer if loop ends
    if buffer and state["daily_bytes"] < DAILY_LIMIT_BYTES:
        success, bytes_added = await transfer_bundle(buffer, state)
        if success:
            state["last_id"] = buffer[-1].id
            state["daily_bytes"] += bytes_added
            save_state(state)
            items_count += len(buffer)

    print(f"\n🎉 Session terminated securely.")
    print(f"📈 Total items processed this run: {items_count}")
    print(f"📦 Total data pushed today: {state['daily_bytes'] / (1024**3):.2f} GB")

    await user_client.disconnect()
    await bot_client.disconnect()

await run_clone()
