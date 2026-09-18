import os
import asyncio
import logging
from google.colab import userdata, drive
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from tqdm.asyncio import tqdm

# ==========================================
# 0. MOUNT GOOGLE DRIVE & SILENCE NOISE
# ==========================================
drive.mount('/content/drive')

# Mute Telethon warnings and Python 3.13 asyncio task cancellation noise
logging.getLogger('telethon').setLevel(logging.ERROR)
logging.getLogger('asyncio').setLevel(logging.CRITICAL)

# ==========================================
# 1. CONFIGURATION
# ==========================================
API_ID = 1234567                   # Replace with your API_ID
API_HASH = "c6b78f70a981f8fce3271721f508ce59"         # Replace with your API_HASH
TELETHON_SESSION = userdata.get('TELETHON_SESSION')

SOURCE_CHAT = -1002320221806       
DESTINATION_CHAT = -1003846210054  
TOPIC_ID = 261 

CHECKPOINT_DIR = "/content/drive/MyDrive/TelegramClones"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, f"checkpoint_{abs(SOURCE_CHAT)}_{TOPIC_ID or 'channel'}.txt")

# Hardened Network Client
client = TelegramClient(
    StringSession(TELETHON_SESSION), 
    API_ID, 
    API_HASH,
    connection_retries=100,  
    request_retries=100,     
    timeout=60               
)

# ==========================================
# 2. STATE MANAGEMENT (IDEMPOTENCY)
# ==========================================
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
# 3. 8-WORKER HIGH-SPEED PARALLEL DOWNLOAD
# ==========================================
async def fast_download(client, msg, file_path):
    if not getattr(msg, 'document', None) and not getattr(msg, 'photo', None) and not getattr(msg, 'video', None):
        return await client.download_media(msg, file=file_path)

    file_size = 0
    if getattr(msg, 'document', None):
        file_size = msg.document.size
    elif getattr(msg, 'video', None):
        file_size = msg.video.size
    elif getattr(msg, 'photo', None):
        return await client.download_media(msg, file=file_path)

    if file_size < 5 * 1024 * 1024:
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
                        msg.media, 
                        offset=offset, 
                        limit=limit, 
                        chunk_size=chunk_size
                    ):
                        with open(file_path, "r+b") as f:
                            f.seek(offset)
                            f.write(chunk)
                        pbar.update(len(chunk))
                        break
                    break
                except Exception as e:
                    if attempt == 4:
                        raise e
                    await asyncio.sleep(2)

        queue = asyncio.Queue()
        for i in range(total_chunks):
            queue.put_nowait(i)

        async def worker(worker_id):
            while not queue.empty():
                try:
                    chunk_index = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await download_chunk(worker_id, chunk_index)
                queue.task_done()

        tasks = [asyncio.create_task(worker(i)) for i in range(WORKERS)]
        await asyncio.gather(*tasks)

    return file_path

# ==========================================
# 4. MESSAGE TRANSFER ENGINE (FAST DL + FAST UPLOAD)
# ==========================================
async def transfer_message(msg):
    file_path = None
    media_sent = False
    
    try:
        if getattr(msg, 'action', None):
            return True

        if getattr(msg, 'file', None):
            file_title = getattr(msg.file, 'name', None)
            if not file_title and hasattr(msg, 'document') and msg.document:
                for attr in msg.document.attributes:
                    if hasattr(attr, 'title') and attr.title:
                        file_title = attr.title
                        break
                    elif hasattr(attr, 'file_name') and attr.file_name:
                        file_title = attr.file_name
                        break

            caption = msg.text or file_title or ""
            file_title_fallback = file_title or f"media_{msg.id}.mp4"
            file_path = os.path.join(os.getcwd(), file_title_fallback)
            
            log_preview = (msg.text.strip().split('\n')[0][:50] + "...") if msg.text else (file_title or "Untitled Media")
            print(f"\n[PROCESSING] ID: {msg.id} | \"{log_preview}\"")
            
            # 1. 8-Worker Parallel Download (~9 MB/s)
            for dl_attempt in range(3):
                try:
                    file_path = await fast_download(client, msg, file_path)
                    if file_path and os.path.exists(file_path):
                        break 
                except Exception as e:
                    print(f"⚠️ Download drop (Attempt {dl_attempt + 1}/3): {e}. Retrying...")
                    await asyncio.sleep(5)
            
            # 2. Optimized Fast Upload (2MB block chunking for high throughput)
            if file_path and os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                
                with tqdm(total=file_size, desc="📤 Fast Upload", unit='B', unit_scale=True, unit_divisor=1024, colour="green") as pbar:
                    last_sent = [0]
                    
                    def upload_progress(current, total):
                        pbar.update(current - last_sent[0])
                        last_sent[0] = current

                    original_attributes = None
                    if getattr(msg, 'document', None):
                        original_attributes = msg.document.attributes

                    for attempt in range(3):
                        try:
                            # Stream upload with large 2MB part sizing to break past the 1 Mbps floor safely
                            with open(file_path, 'rb') as f:
                                uploaded_file = await client.upload_file(
                                    f,
                                    file_name=os.path.basename(file_path),
                                    part_size_kb=512,
                                    progress_callback=upload_progress
                                )

                            await client.send_file(
                                DESTINATION_CHAT,
                                file=uploaded_file,
                                caption=caption,
                                formatting_entities=msg.entities if msg.text else None,
                                attributes=original_attributes,
                                supports_streaming=True
                            )
                            print(f"\n✅ Transferred Media {msg.id} successfully.")
                            media_sent = True
                            break
                        except errors.FloodWaitError as e:
                            print(f"\n[RATE LIMIT] Pausing for {e.seconds}s...")
                            await asyncio.sleep(e.seconds)
                        except Exception as e:
                            print(f"\n⚠️ Upload attempt {attempt + 1} failed: {e}")
                            await asyncio.sleep(5)
            else:
                print(f"⚠️ Failed to download file for ID {msg.id}.")

        if not media_sent and msg.text:
            log_preview = msg.text.strip().split('\n')[0][:60]
            print(f"\n[DOWNLOADING] Text ID: {msg.id} | Content: \"{log_preview}\"")
            for attempt in range(3):
                try:
                    await client.send_message(
                        DESTINATION_CHAT, 
                        msg.text,
                        formatting_entities=msg.entities
                    )
                    print(f"✅ Transferred Text {msg.id}")
                    break
                except errors.FloodWaitError as e:
                    print(f"[RATE LIMIT] Pausing for {e.seconds}s...")
                    await asyncio.sleep(e.seconds)
                    
        return True

    except Exception as e:
        print(f"[ERROR] Failed on ID {msg.id}: {e}")
        return False
        
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

# ==========================================
# 5. HYBRID EXECUTION PIPELINE
# ==========================================
async def run_clone():
    await client.start()
    print("✅ Connected to Telethon Engine.")

    try:
        source_entity = await client.get_entity(SOURCE_CHAT)
        mode_label = f"Topic {TOPIC_ID}" if TOPIC_ID else "Standard Channel"
        print(f"Targeting: {source_entity.title} | Mode: {mode_label}")
    except ValueError:
        print(f"❌ Error: Account not found in {SOURCE_CHAT}.")
        return

    last_id = get_last_processed_id()
    if last_id > 0:
        print(f"🔄 Resuming from Google Drive checkpoint (Skipping up to ID {last_id})")
    else:
        print("🚀 Starting full clone from the beginning...")

    count = 0

    if TOPIC_ID:
        if last_id < TOPIC_ID:
            root_msg = await client.get_messages(SOURCE_CHAT, ids=TOPIC_ID)
            if root_msg and getattr(root_msg, 'id', None):
                success = await transfer_message(root_msg)
                if success:
                    update_checkpoint(root_msg.id)
                    count += 1

        async for msg in client.iter_messages(SOURCE_CHAT, reply_to=TOPIC_ID, reverse=True):
            if msg.id > last_id and msg.id != TOPIC_ID:
                success = await transfer_message(msg)
                if success:
                    update_checkpoint(msg.id)
                    count += 1
                await asyncio.sleep(1.0)
    else:
        async for msg in client.iter_messages(SOURCE_CHAT, reverse=True, min_id=last_id):
            success = await transfer_message(msg)
            if success:
                update_checkpoint(msg.id)
                count += 1
            await asyncio.sleep(1.0)

    print(f"\n🎉 Clone complete. Transferred {count} new items.")

await run_clone()
