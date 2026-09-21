import sys
import subprocess
import os
import asyncio
import logging
import random
from google.colab import userdata, drive
import nest_asyncio

print("🔄 Ensuring required dependencies are installed...")
subprocess.check_call([sys.executable, "-m", "pip", "install", "telethon", "cryptg", "tqdm", "nest_asyncio", "-q"])

import cryptg
from telethon import TelegramClient, errors, functions, types, utils
from telethon.sessions import StringSession
from tqdm.asyncio import tqdm

# ==========================================
# 0. SETUP & MOUNT
# ==========================================
nest_asyncio.apply()
drive.mount('/content/drive')

logging.getLogger('telethon').setLevel(logging.ERROR)
logging.getLogger('asyncio').setLevel(logging.CRITICAL)

API_ID =           # Replace with your API_ID
API_HASH = "  " # Replace with your API_HASH
TELETHON_SESSION = userdata.get('TELETHON_SESSION')

SOURCE_CHAT = -1002320221806
DESTINATION_CHAT = -1003846210054
TOPIC_ID = 261

CHECKPOINT_DIR = "/content/drive/MyDrive/TelegramClones"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, f"checkpoint_{abs(SOURCE_CHAT)}_{TOPIC_ID or 'channel'}.txt")

client = TelegramClient(
    StringSession(TELETHON_SESSION),
    API_ID,
    API_HASH,
    connection_retries=100,
    request_retries=100,
    timeout=60
)

def get_last_processed_id():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            content = f.read().strip()
            if content.isdigit():
                return int(content)
    return 0

def update_checkpoint(msg_id):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(msg_id))

# ==========================================
# 1. FAST DOWNLOADER (+ ANTI-BAN PROTECTED)
# ==========================================
async def fast_download(client, msg, file_path):
    if not getattr(msg, 'document', None) and not getattr(msg, 'photo', None) and not getattr(msg, 'video', None):
        return await client.download_media(msg, file=file_path)

    file_size = getattr(msg.document, 'size', getattr(msg.video, 'size', 0)) if hasattr(msg, 'document') or hasattr(msg, 'video') else 0

    if getattr(msg, 'photo', None) or file_size < 5 * 1024 * 1024:
        return await client.download_media(msg, file=file_path)

    WORKERS = 8
    chunk_size = 512 * 1024
    total_chunks = (file_size + chunk_size - 1) // chunk_size

    if os.path.exists(file_path):
        os.remove(file_path)

    with open(file_path, "wb") as f:
        f.truncate(file_size)

    with tqdm(total=file_size, desc="📥 Downloading", unit='B', unit_scale=True, unit_divisor=1024, colour="blue") as pbar:
        async def download_chunk(worker_id, chunk_index):
            offset = chunk_index * chunk_size
            limit = min(chunk_size, file_size - offset)

            for attempt in range(5):
                try:
                    async for chunk in client.iter_download(
                        msg.media, offset=offset, limit=limit, chunk_size=chunk_size
                    ):
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
        for i in range(total_chunks):
            queue.put_nowait(i)

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
# 2. PARALLEL UPLOADER (+ ANTI-BAN PROTECTED)
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

    with tqdm(total=file_size, desc="📤 Parallel Upload", unit='B', unit_scale=True, colour="green") as pbar:
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
async def transfer_bundle(msgs):
    uploaded_media = []
    captions = []
    original_attributes = []
    current_file_path = None
    bundle_bytes_added = 0

    try:
        for msg in msgs:
            if getattr(msg, 'action', None):
                continue

            if not getattr(msg, 'media', None):
                for send_attempt in range(5):
                    try:
                        await client.send_message(DESTINATION_CHAT, msg.text, formatting_entities=msg.entities)
                        print(f"✅ Transferred Text {msg.id}")
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

            attr = msg.document.attributes if getattr(msg, 'document', None) else None
            original_attributes.append(attr)

            log_preview = (msg.text.strip().split('\n')[0][:40] + "...") if msg.text else file_title_fallback
            lbl = "ALBUM PART" if len(msgs) > 1 else "SINGLE"
            print(f"\n[{lbl}] ID: {msg.id} | \"{log_preview}\"")

            for dl_attempt in range(3):
                try:
                    current_file_path = await fast_download(client, msg, current_file_path)
                    if current_file_path and os.path.exists(current_file_path): break
                except Exception as e:
                    print(f"⚠️ DL drop (Attempt {dl_attempt + 1}/3): {e}. Retrying...")
                    await asyncio.sleep(5)

            if current_file_path and os.path.exists(current_file_path):
                file_size = os.path.getsize(current_file_path)
                uploaded_file = await safe_parallel_upload(client, current_file_path, workers=4)
                uploaded_media.append(uploaded_file)
                captions.append(msg.text or "")
                bundle_bytes_added += file_size

                os.remove(current_file_path)
                current_file_path = None

        if uploaded_media:
            for send_attempt in range(5):
                try:
                    if len(uploaded_media) == 1:
                        await client.send_file(
                            DESTINATION_CHAT,
                            file=uploaded_media[0],
                            caption=captions[0],
                            formatting_entities=msgs[0].entities,
                            attributes=original_attributes[0],
                            supports_streaming=True
                        )
                    else:
                        await client.send_file(
                            DESTINATION_CHAT,
                            file=uploaded_media,
                            caption=captions,
                            supports_streaming=True
                        )

                    log_id = msgs[0].id if len(msgs) == 1 else f"{msgs[0].id}-{msgs[-1].id}"
                    print(f"✅ Successfully Committed Media {log_id}")
                    break
                except errors.FloodWaitError as e:
                    print(f"⏳ [RATE LIMIT] Pausing for {e.seconds}s...")
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

# ==========================================
# 4. EXECUTION PIPELINE (WITH DETAILED SAFETY LOGS)
# ==========================================
async def run_clone():
    await client.start()
    last_id = get_last_processed_id()
    print(f"🚀 Connected. Resuming from ID {last_id}")

    items_count = 0
    total_bytes_today = 0
    DAILY_LIMIT_GB = 40
    DAILY_LIMIT_BYTES = DAILY_LIMIT_GB * 1024 * 1024 * 1024

    buffer = []
    current_group = None
    kwargs = {'reverse': True, 'min_id': last_id}
    if TOPIC_ID: kwargs['reply_to'] = TOPIC_ID

    print(f"🛡️ [ANTI-BAN ACTIVE] Daily Volume Ceiling set to: {DAILY_LIMIT_GB} GB")
    print(f"🛡️ [ANTI-BAN ACTIVE] Jitter pacing and 15-file milestone breaks enabled.\n")

    async for msg in client.iter_messages(SOURCE_CHAT, **kwargs):
        if msg.id == TOPIC_ID: continue

        # --- SAFETY CIRCUIT BREAKER (40 GB VOLUME LIMIT) ---
        if total_bytes_today >= DAILY_LIMIT_BYTES:
            print(f"\n🛑 [SAFETY LOCK TRIGGERED] Daily volume limit of {DAILY_LIMIT_GB} GB has been reached.")
            print(f"📊 Total transferred this session: {total_bytes_today / (1024**3):.2f} GB.")
            print("🛡️ Gracefully exiting to protect your 2-year account trust score. Run again tomorrow!")
            break

        if msg.grouped_id:
            if current_group == msg.grouped_id:
                buffer.append(msg)
            else:
                if buffer:
                    success, bytes_added = await transfer_bundle(buffer)
                    if not success:
                        print("❌ Bundle transfer failed. Breaking loop safely.")
                        break
                    update_checkpoint(buffer[-1].id)
                    items_count += len(buffer)
                    total_bytes_today += bytes_added
                    print(f"📊 [PROGRESS] Items: {items_count} | Volume: {total_bytes_today / (1024**3):.2f} GB / {DAILY_LIMIT_GB} GB")
                buffer = [msg]
                current_group = msg.grouped_id
        else:
            if buffer:
                success, bytes_added = await transfer_bundle(buffer)
                if not success:
                    print("❌ Bundle transfer failed. Breaking loop safely.")
                    break
                update_checkpoint(buffer[-1].id)
                items_count += len(buffer)
                total_bytes_today += bytes_added
                buffer = []
                current_group = None
                print(f"📊 [PROGRESS] Items: {items_count} | Volume: {total_bytes_today / (1024**3):.2f} GB / {DAILY_LIMIT_GB} GB")

            success, bytes_added = await transfer_bundle([msg])
            if not success:
                print("❌ Single item transfer failed. Breaking loop safely.")
                break
            update_checkpoint(msg.id)
            items_count += 1
            total_bytes_today += bytes_added
            print(f"📊 [PROGRESS] Items: {items_count} | Volume: {total_bytes_today / (1024**3):.2f} GB / {DAILY_LIMIT_GB} GB")

        # --- DEFENSE 1: BATCH MILESTONE PAUSES (WITH LOGS) ---
        if items_count > 0 and items_count % 15 == 0:
            pause_time = random.randint(30, 45)
            print(f"\n☕ [MILESTONE PAUSE] Completed {items_count} items. Resting for {pause_time}s to simulate human workflow...")
            await asyncio.sleep(pause_time)
            print(f"🔄 Resuming transfer pipeline...")

        # --- DEFENSE 2: RANDOMIZED SLEEP JITTER (WITH LOGS) ---
        jitter_delay = random.uniform(2.5, 5.5)
        print(f"💤 [JITTER] Pacing delay applied: {jitter_delay:.2f}s")
        await asyncio.sleep(jitter_delay)

    if buffer and total_bytes_today < DAILY_LIMIT_BYTES:
        success, bytes_added = await transfer_bundle(buffer)
        if success:
            update_checkpoint(buffer[-1].id)
            items_count += len(buffer)
            total_bytes_today += bytes_added

    print(f"\n🎉 Session terminated successfully.")
    print(f"📈 Total items processed: {items_count}")
    print(f"📦 Total data pushed today: {total_bytes_today / (1024**3):.2f} GB")
    await client.disconnect()

await run_clone()
