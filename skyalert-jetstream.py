import asyncio
from atproto import Client, IdResolver, models, SessionEvent, AsyncClient
import atproto_client
import json
import atproto_client.exceptions
import websockets
import yaml
import os
import datetime
import time
import re
import aiohttp
import aiofiles

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
CONFIG_FILE = os.path.join(DATA_DIR, 'config.yaml')
CACHE_DIR = os.path.join(DATA_DIR, 'cache')
LAST_RUN_FILE = os.path.join(DATA_DIR, 'last_run.txt')
VERBOSE_PRINTING = False

global client
client = AsyncClient()

async def on_session_change(event: atproto_client.SessionEvent, session: atproto_client.Session):
    if VERBOSE_PRINTING: print(f"Session change event: {event}")
    if event == SessionEvent.CREATE or event == SessionEvent.REFRESH:
        async with aiofiles.open(os.path.join(DATA_DIR, 'login-info.yaml'), 'r') as f1:
            login_info = yaml.safe_load(await f1.read())
            async with aiofiles.open(os.path.join(DATA_DIR, 'login-info.yaml'), 'w') as f2:
                new_login_info = {
                    'username': login_info['username'],
                    'password': login_info['password'],
                    'session-key-firehose': session.export(),
                    'session-key-cmds': login_info['session-key-cmds'] if 'session-key-cmds' in login_info else ""
                }
                await f2.write(yaml.dump(new_login_info))

client.on_session_change = on_session_change

async def load_login_info():
    if VERBOSE_PRINTING: print("Loading login info...")
    async with aiofiles.open(os.path.join(DATA_DIR, 'login-info.yaml'), 'r') as f:
        login_info = yaml.safe_load(await f.read())
        if 'session-key-firehose' in login_info and login_info['session-key-firehose']:
            await client.login(session_string=login_info['session-key-firehose'])
        else:
            await client.login(login=login_info['username'], password=login_info['password'])
    if VERBOSE_PRINTING: print("Login info loaded.")

# Link detection by latchk3y on the Bluesky API Discord server
def get_facets(text):
    if VERBOSE_PRINTING: print(f"Extracting facets from text: {text}")
    pattern = r'(https?://[^\s]+)'
    links = re.findall(pattern, text)

    facets = []
    
    for link in links:
        start_index = text.index(link)
        end_index = start_index + len(link)
        
        facets.append({
            "index": {
                "byteStart": start_index,
                "byteEnd": end_index              },
            "features": [
                {
                    "$type": "app.bsky.richtext.facet#link",
                    "uri": link
                }
            ]
        })

    if not facets:
        return None  

    if VERBOSE_PRINTING: print(f"Facets extracted: {facets}")
    return facets

async def get_config():
    if VERBOSE_PRINTING: print("Loading config...")
    if not os.path.exists(CONFIG_FILE):
        return {'user_watches': [], 'follow_watches': []}
    async with aiofiles.open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(await f.read())
        if VERBOSE_PRINTING: print("Config loaded.")
        return config
    
async def save_config(config):
    if VERBOSE_PRINTING: print("Saving config...")
    async with aiofiles.open(CONFIG_FILE, 'w') as f:
        await f.write(yaml.dump(config))
    if VERBOSE_PRINTING: print("Config saved.")

async def get_last_run():
    if VERBOSE_PRINTING: print("Getting last run time...")
    if not os.path.exists(LAST_RUN_FILE):
        return None
    async with aiofiles.open(LAST_RUN_FILE, 'r') as f:
        last_run = datetime.datetime.fromisoformat(await f.read())
        if VERBOSE_PRINTING: print(f"Last run time: {last_run}")
        return last_run
        
async def save_last_run():
    if VERBOSE_PRINTING: print("Saving last run time...")
    async with aiofiles.open(LAST_RUN_FILE, 'w') as f:
        await f.write(datetime.datetime.now(datetime.timezone.utc).isoformat())
    if VERBOSE_PRINTING: print("Last run time saved.")
    
def post_url_from_at_uri(at_uri):
    if VERBOSE_PRINTING: print(f"Generating post URL from AT URI: {at_uri}")
    # Split the AT URI to extract the DID and the random string
    parts = at_uri.split('/')
    did = parts[2]
    random_string = parts[-1]
    
    # Construct the Bluesky URL
    url = f"https://bsky.app/profile/{did}/post/{random_string}"
    if VERBOSE_PRINTING: print(f"Generated post URL: {url}")
    return url

def bridgy_to_fed(handle: str):
    if VERBOSE_PRINTING: print(f"Converting Bridgy handle to Fediverse handle: {handle}")
    if handle.endswith("ap.brid.gy"):
        parts = handle.split('.')
        if len(parts) >= 3:
            username = parts[0]
            domain = '.'.join(parts[1:-3])
            converted_handle = f"@{username}@{domain} (Bridgy)"
        else:
            converted_handle = f"@{handle}"
    else:
        if handle.endswith("bsky.social"):
            handle = handle[:-12]  # Remove ".bsky.social" from the end
        converted_handle = handle
    if VERBOSE_PRINTING: print(f"Converted handle: {converted_handle}")
    return converted_handle
    
def fed_to_bridgy(handle: str):
    if VERBOSE_PRINTING: print(f"Converting Fediverse handle to Bridgy handle: {handle}")
    if "@" in handle:
        parts = handle.split('@')
        if len(parts) >= 3:  # If given a Fediverse handle, output the Bridgy handle
            username = parts[1]
            domain = '.'.join(parts[2:])
            converted_handle = f"{username}.{domain}.ap.brid.gy"
        else:  # If given a normal Bluesky handle, output the handle ensuring there is no @ at the beginning
            converted_handle = handle.lstrip('@')
    else:
        if not handle.endswith("bsky.social") and '.' not in handle:
            handle += ".bsky.social"
        converted_handle = handle
    if VERBOSE_PRINTING: print(f"Converted handle: {converted_handle}")
    return converted_handle

async def send_dm(to, message):
    if VERBOSE_PRINTING: print(f"Sending DM to {to}: {message}")
    dm_client = client.with_bsky_chat_proxy()
    dm = dm_client.chat.bsky.convo
    
    id_resolver = IdResolver()
    chat_to = to if "did:plc:" in to else await id_resolver.handle.resolve(to)

    convo = await dm.get_convo_for_members(
        models.ChatBskyConvoGetConvoForMembers.Params(members=[chat_to, client.me.did]),
    ).convo
    
    await dm.send_message(
        models.ChatBskyConvoSendMessage.Data(
            convo_id=convo.id,
            message=models.ChatBskyConvoDefs.MessageInput(
                text=message,
                facets=get_facets(message)
            ),
        )
    )

    if VERBOSE_PRINTING: print("DM sent.")
    
# main logic
async def main(uri):
    if VERBOSE_PRINTING: print("Starting main logic...")
    if VERBOSE_PRINTING: print("Signing into Bluesky...")
    await load_login_info()
    
    if VERBOSE_PRINTING: print(f"Connecting to WebSocket URI: {uri}")
    async with websockets.connect(uri) as websocket:
        while True:
            message = await websocket.recv()
            if VERBOSE_PRINTING: print(f"Received message: {message}")
            message_dict = json.loads(message)
            commit = message_dict.get("commit")
            if commit:
                if VERBOSE_PRINTING: print("Processing commit...")
                config = await get_config()
                for watch in config['user_watches']:
                    if watch['subject-did'] in message_dict['did']:
                        if commit.get("collection") == "app.bsky.feed.post":
                            message1 = f"{bridgy_to_fed(watch['subject-handle'])} said:\n{commit.get('record').get('text')}"
                            
                            embed = commit.get("record").get("embed")
                            
                            if commit.get("record").get("labels"):
                                message1 += " [content warning]"
                            
                            if embed:
                                if embed.get("$type") == "app.bsky.embed.images":
                                    message1 += " [has images]"
                                if embed.get("$type") == "app.bsky.embed.video":
                                    message1 += " [has video]"
                                if embed.get("$type") == "app.bsky.embed.external":
                                    if "tenor.com" in embed.get("external").get("uri"):
                                        message1 += " [has GIF]"
                                    else:
                                        message1 += " [link preview]"
                                if embed.get("$type") == "app.bsky.embed.record":
                                    message1 += " [quote repost]"
                                    
                            post_url = f"https://bsky.app/profile/{message_dict.get('did')}/post/{commit.get('rkey')}"
                            message2 = f"Link to post: {post_url}"
                            await send_dm(watch['receiver-did'], message1)
                            await send_dm(watch['receiver-did'], message2)
                        elif commit.get("collection") == "app.bsky.feed.repost":
                            post = (await client.get_posts([commit.get("record").get("uri")]))[0]
                            message1 = f"{bridgy_to_fed(watch['subject-handle'])} reposted {post.author.handle} saying:\n{post.text}"
                            message2 = f"Link to post: {post_url_from_at_uri(post.uri)}"
                            await send_dm(watch['receiver-did'], message1)
                            await send_dm(watch['receiver-did'], message2)
                if VERBOSE_PRINTING: print("Commit processed.")

if __name__ == "__main__":
    uri = "wss://jetstream2.us-east.bsky.network/subscribe"  # Replace with your WebSocket URI
    asyncio.run(main(uri))