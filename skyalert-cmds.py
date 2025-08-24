from atproto import Client, IdResolver, models, SessionEvent
import atproto_client
import json
import atproto_client.exceptions
import yaml
import os
import datetime
import time
import re
import tenacity

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
CONFIG_FILE = os.path.join(DATA_DIR, 'config.yaml')
CACHE_DIR = os.path.join(DATA_DIR, 'cache')
LAST_RUN_FILE = os.path.join(DATA_DIR, 'last_run.txt')
VERBOSE_PRINTING = True
MAINTAINER_DIDS = ["did:plc:big6e357j2bbrlkyms5vjkgf"]

global client
client = Client()

@client.on_session_change
def on_session_change(event: atproto_client.SessionEvent,session: atproto_client.Session):
    if event==SessionEvent.CREATE or event==SessionEvent.REFRESH:
        with open(os.path.join(DATA_DIR, 'login-info.yaml'), 'r') as f1:
            login_info = yaml.safe_load(f1)
            with open(os.path.join(DATA_DIR, 'login-info.yaml'), 'w') as f2:
                new_login_info = {
                    'username': login_info['username'],
                    'password': login_info['password'],
                    'session-key-firehose': session.export(),
                    'session-key-cmds': login_info['session-key-cmds'] if 'session-key-cmds' in login_info else ""
                }
                yaml.dump(new_login_info, f2)

with open(os.path.join(DATA_DIR, 'login-info.yaml'), 'r') as f:
    login_info = yaml.safe_load(f)

    if 'session-key-firehose' in login_info and login_info['session-key-firehose']:
        client.login(session_string=login_info['session-key-firehose'])
    else:
        client.login(login=login_info['username'], password=login_info['password'])

# Link detection by latchk3y on the Bluesky API Discord server
def get_facets_from_links(text):
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

    return facets

def get_facets_from_markdown(text):
    pattern = r'\[([^\]]+)\]\((https?://[^\s]+)\)'
    facets = []
    filtered_text = text

    match = re.search(pattern, filtered_text)
    while match:
        link_text, link_url = match.groups()
        start_index = match.start()
        end_index = match.end()

        filtered_text = filtered_text[:start_index] + link_text + filtered_text[end_index:]
        new_start_index = start_index
        new_end_index = new_start_index + len(link_text)

        facets.append({
            "index": {
                "byteStart": new_start_index,
                "byteEnd": new_end_index
            },
            "features": [
                {
                    "$type": "app.bsky.richtext.facet#link",
                    "uri": link_url
                }
            ]
        })

        match = re.search(pattern, filtered_text)

    # Extract links from the filtered text
    link_pattern = r'(https?://[^\s]+)'
    links = re.findall(link_pattern, filtered_text)

    for link in links:
        start_index = filtered_text.index(link)
        end_index = start_index + len(link)
        
        facets.append({
            "index": {
                "byteStart": start_index,
                "byteEnd": end_index
            },
            "features": [
                {
                    "$type": "app.bsky.richtext.facet#link",
                    "uri": link
                }
            ]
        })

    return {"facets": facets, "filtered_text": filtered_text}

def get_config():
    if not os.path.exists(CONFIG_FILE):
        return {'user_watches': [], 'follow_watches': []}
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)
        return config
    
def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        yaml.dump(config, f)
        
def get_last_run():
    if not os.path.exists(LAST_RUN_FILE):
        return None
    with open(LAST_RUN_FILE, 'r') as f:
        return datetime.datetime.fromisoformat(f.read())
        
def save_last_run():
    with open(LAST_RUN_FILE, 'w') as f:
        f.write(datetime.datetime.now(datetime.timezone.utc).isoformat())
        
def firehose_check():
    if os.path.exists(os.path.join(DATA_DIR, 'last_run-firehose.txt')):
        with open(os.path.join(DATA_DIR, 'last_run-firehose.txt'), 'r') as f:
            last_run = datetime.datetime.fromisoformat(f.read())
            if datetime.datetime.now(datetime.timezone.utc) - last_run > datetime.timedelta(minutes=10):
                return True
            else:
                return False
    
def post_url_from_at_uri(at_uri):
    # Split the AT URI to extract the DID and the random string
    parts = at_uri.split('/')
    did = parts[2]
    random_string = parts[-1]
    
    # Construct the Bluesky URL
    url = f"https://bsky.app/profile/{did}/post/{random_string}"
    return url

def bridgy_to_fed(handle: str):
    if handle.endswith("ap.brid.gy"):
        parts = handle.split('.')
        if len(parts) >= 3:
            username = parts[0]
            domain = '.'.join(parts[1:-3])
            return f"@{username}@{domain} (Bridgy)"
        else:
            return f"@{handle}"
    else:
        if handle.endswith("bsky.social"):
            handle = handle[:-12]  # Remove ".bsky.social" from the end
        return handle
    
def fed_to_bridgy(handle: str):
    if "@" in handle:
        parts = handle.split('@')
        if len(parts) >= 3:  # If given a Fediverse handle, output the Bridgy handle
            username = parts[1]
            domain = '.'.join(parts[2:])
            return f"{username}.{domain}.ap.brid.gy"
        else:  # If given a normal Bluesky handle, output the handle ensuring there is no @ at the beginning
            return handle.lstrip('@')
    else:
        if not handle.endswith("bsky.social") and '.' not in handle:
            handle += ".bsky.social"
        return handle

def send_dm(to,message):
    dm_client = client.with_bsky_chat_proxy()
    dm = dm_client.chat.bsky.convo
    
    # create resolver instance with in-memory cache
    id_resolver = IdResolver()
    # resolve DID
    chat_to = to if "did:plc:" in to else id_resolver.handle.resolve(to)

    # create or get conversation with chat_to
    convo = dm.get_convo_for_members(
        models.ChatBskyConvoGetConvoForMembers.Params(members=[chat_to, client.me.did]),
    ).convo
    
    # filter markdown links from text and get facets
    content = get_facets_from_markdown(message)
    
    # send a message to the conversation
    dm.send_message(
        models.ChatBskyConvoSendMessage.Data(
            convo_id=convo.id,
            message=models.ChatBskyConvoDefs.MessageInput(
                text=content["filtered_text"],
                facets=content["facets"]
            ),
        )
    )

    if VERBOSE_PRINTING: print('\nMessage sent!')

# logic for handling bot commands
def bot_commands_handler():
    dm_client = client.with_bsky_chat_proxy()
    dm = dm_client.chat.bsky.convo
    
    if VERBOSE_PRINTING: print("Checking for bot commands...")
    # check for commands sent to the bot
    dmconvos_objs = dm.list_convos().model_dump()
    dmconvos = []
    dmconvos.extend(dmconvos_objs['convos'])
    while True:
        if "cursor" not in dmconvos_objs or dmconvos_objs["cursor"] is None:
            break
        dmconvos.extend(dmconvos_objs['convos'])
        dmconvos_objs = dm.list_convos(cursor=dmconvos_objs['cursor']).model_dump()
    
    for convo in dmconvos:
        if convo['last_message']['sender']['did'] == client.me.did:
            continue # The bot sent the last message, skip
        else:
            senderprofile = client.get_profile(convo['last_message']['sender']['did']).model_dump()
            senderhandle = senderprofile['handle']
            
            if convo['last_message']['text'].lower() == "!help":
                if VERBOSE_PRINTING: print(f"Sending help message to {senderhandle}...")
                message = "SkyAlert is a bot that can notify you about posts from people you watch or if someone unfollows you. To set up a watch, send me a DM with the following commands:\n\n!watch <subject> [reposts-allowed] - Watch a subject for new posts. You will be notified when the subject posts. If reposts-allowed is true, you will be notified on reposts.\n!unwatch <subject> - Stop watching a subject.\n!mywatches - List the subjects you are watching and the status of the follow watch feature.\n!repost-default <true/false> - Set the default reposts-allowed setting for new watches.\n!followwatch <true/false> - Enable or disable notifications for unfollows. You will be notified when someone unfollows you.\n!replies <true/false> - If this is true, you will see replies posted by the subjects you are watching.\n!reset - Delete all of your SkyAlert settings."
                send_dm(convo['last_message']['sender']['did'], message)
            elif convo['last_message']['text'].lower().startswith("!watch"):
                if VERBOSE_PRINTING: print(f"Processing watch command from {senderhandle}...")
                parts = convo['last_message']['text'].split(' ')
                if len(parts) < 2:
                    message = "Not enough arguments. Usage: !watch <subject> [reposts-allowed]"
                    send_dm(convo['last_message']['sender']['did'], message)
                else:
                    config = get_config()
                    subject = fed_to_bridgy(parts[1])
                    reposts_allowed = False
                    if len(parts) == 3:
                        reposts_allowed = parts[2].lower() == "true"
                    else:
                        for entry in config.get('repost_defaults', []):
                            if entry['did'] == convo['last_message']['sender']['did']:
                                reposts_allowed = entry['reposts-allowed']
                                break
                    
                    if not IdResolver().handle.resolve(subject):
                        message = ""
                        if subject.endswith("ap.brid.gy"):
                            message = f"Invalid subject handle. You entered a Fediverse or Bridgy Fed handle; that user may not be using Bridgy Fed."
                        elif not subject.endswith(".bsky.social"):
                            message = f"Invalid subject handle. Most Bluesky handles end in .bsky.social, try adding that."
                        else:
                            message = f"Invalid subject handle. There was no known reason the handle could be invalid, so that user likely does not exist or you made a typo."
                        
                        send_dm(convo['last_message']['sender']['did'], message)
                        continue
                    
                    config['user_watches'].append({'subject-handle': subject, 'receiver-handle': senderhandle, 'reposts-allowed': reposts_allowed, 'subject-did': IdResolver().handle.resolve(subject), 'receiver-did': convo['last_message']['sender']['did']})
                    save_config(config)
                    message = f"Watching {bridgy_to_fed(subject)} for new posts. Reposts allowed: {reposts_allowed}. You will be notified when the subject posts."
                    send_dm(convo['last_message']['sender']['did'], message)
            elif convo['last_message']['text'].lower().startswith("!unwatch"):
                if VERBOSE_PRINTING: print(f"Processing unwatch command from {senderhandle}...")
                parts = convo['last_message']['text'].split(' ')
                if len(parts) != 2 or parts[1] == "":
                    message = "Not enough arguments. Usage: !unwatch <subject>"
                    send_dm(convo['last_message']['sender']['did'], message)
                else:
                    config = get_config()
                    subject_handle = fed_to_bridgy(parts[1])
                    receiver_handle = senderhandle
                    user_watches = config.get('user_watches', [])
                    new_user_watches = [watch for watch in user_watches if not (watch['subject-handle'] == subject_handle and watch['receiver-handle'] == receiver_handle)]
                    
                    if len(user_watches) == len(new_user_watches):
                        message = f"No watch found for {bridgy_to_fed(subject_handle)}."
                    else:
                        config['user_watches'] = new_user_watches
                        save_config(config)
                        message = f"Stopped watching {bridgy_to_fed(subject_handle)}."
                    
                    send_dm(convo['last_message']['sender']['did'], message)
            elif convo['last_message']['text'].lower() == "!mywatches":
                if VERBOSE_PRINTING: print(f"Processing mywatches command from {senderhandle}...")
                config = get_config()
                follow_watches = config.get('follow_watches', [])
                user_watches = config.get('user_watches', [])

                # Check follow watch status
                follow_watch_status = "disabled"
                for watch in follow_watches:
                    if watch['did'] == convo['last_message']['sender']['did']:
                        follow_watch_status = "enabled"
                        break
                message = f"Follow watch notifications are {follow_watch_status}.\n\n"

                # List user watches
                user_watch_list = [watch for watch in user_watches if watch['receiver-handle'] == senderhandle]
                if user_watch_list:
                    message += "You are watching the following subjects:\n"
                    lines = []
                    for watch in user_watch_list:
                        lines.append(f"- {bridgy_to_fed(watch['subject-handle'])} (Reposts allowed: {watch['reposts-allowed']})")
                    message += "\n".join(lines)
                else:
                    message += "You are not watching any subjects."

                send_dm(convo['last_message']['sender']['did'], message)
            elif convo['last_message']['text'].lower().startswith("!repost-default"):
                if VERBOSE_PRINTING: print(f"Processing repost-default command from {senderhandle}...")
                parts = convo['last_message']['text'].split(' ')
                if len(parts) != 2 or parts[1] == "":
                    current_default = next((entry['reposts-allowed'] for entry in get_config().get('repost_defaults', []) if entry['did'] == convo['last_message']['sender']['did']), None)
                    message = f"Not enough arguments. Usage: !repost-default <true/false>\nCurrent default setting: {current_default}"
                    send_dm(convo['last_message']['sender']['did'], message)
                else:
                    config = get_config()
                    repost_default = parts[1].lower() == "true"
                    repost_defaults = config.get('repost_defaults', [])
                    
                    if any(entry['did'] == convo['last_message']['sender']['did'] for entry in repost_defaults):
                        repost_defaults = [entry for entry in repost_defaults if entry['did'] != convo['last_message']['sender']['did']]
                    
                    repost_defaults.append({'did': convo['last_message']['sender']['did'], 'reposts-allowed': repost_default})
                    config['repost_defaults'] = repost_defaults
                    save_config(config)
                    message = f"Default reposts-allowed setting set to {repost_default}."
                    send_dm(convo['last_message']['sender']['did'], message)
            elif convo['last_message']['text'].lower().startswith("!followwatch"):
                if VERBOSE_PRINTING: print(f"Processing followwatch command from {senderhandle}...")
                parts = convo['last_message']['text'].split(' ')
                if len(parts) != 2 or parts[1] == "":
                    message = "Not enough arguments. Usage: !followwatch <true/false>"
                    send_dm(convo['last_message']['sender']['did'], message)
                else:
                    config = get_config()
                    followwatch = parts[1].lower() == "true"
                    follow_watches = config.get('follow_watches', [])
                    
                    if followwatch:
                        if not any(watch['did'] == convo['last_message']['sender']['did'] for watch in follow_watches):
                            follow_watches.append({'did': convo['last_message']['sender']['did'], 'handle': senderhandle})
                            message = "Notifications enabled for unfollows."
                        else:
                            message = "Notifications already enabled for unfollows."
                    else:
                        if any(watch['did'] == convo['last_message']['sender']['did'] for watch in follow_watches):
                            follow_watches = [watch for watch in follow_watches if watch['did'] != convo['last_message']['sender']['did']]
                            message = "Notifications disabled for unfollows."
                        else:
                            message = "Notifications already disabled for unfollows."
                    
                    config['follow_watches'] = follow_watches
                    save_config(config)
                    send_dm(convo['last_message']['sender']['did'], message)
            elif convo['last_message']['text'].lower().startswith("!replies"):
                if VERBOSE_PRINTING: print(f"Processing replies command from {senderhandle}...")
                parts = convo['last_message']['text'].split(' ')
                if len(parts) != 2 or parts[1] == "":
                    current_setting = next((entry['replies-allowed'] for entry in get_config().get('reply_settings', []) if entry['did'] == convo['last_message']['sender']['did']), None)
                    message = f"Not enough arguments. Usage: !replies <true/false>\nCurrent setting: {current_setting}"
                    send_dm(convo['last_message']['sender']['did'], message)
                else:
                    config = get_config()
                    replies_allowed = parts[1].lower() == "true"
                    reply_settings = config.get('reply_settings', [])
                    
                    if any(entry['did'] == convo['last_message']['sender']['did'] for entry in reply_settings):
                        reply_settings = [entry for entry in reply_settings if entry['did'] != convo['last_message']['sender']['did']]
                    
                    reply_settings.append({'did': convo['last_message']['sender']['did'], 'replies-allowed': replies_allowed})
                    config['reply_settings'] = reply_settings
                    save_config(config)
                    message = f"Replies allowed setting set to {replies_allowed}."
                    send_dm(convo['last_message']['sender']['did'], message)
            elif convo['last_message']['text'].lower() == "!post-restart":
                if VERBOSE_PRINTING: print(f"Processing post-restart command from {senderhandle}...")
                if convo['last_message']['sender']['did'] in MAINTAINER_DIDS:
                    os.system("systemctl restart skyalert-firehose")
                    message = "Post service restarted."
                    send_dm(convo['last_message']['sender']['did'], message)
                else:
                    message = "You are not LittleBit or someone he trusts. If post notifications have stopped, DM or ping @littlebitstudios.com."
                    send_dm(convo['last_message']['sender']['did'], message)
            elif convo['last_message']['text'].lower() == "!reset":
                if VERBOSE_PRINTING: print(f"Processing reset command from {senderhandle}...")
                message = "This command will completely dissolve your relationship with SkyAlert. All user watches will be removed, follow watch will be disabled, and your preferences will be reset to default. If this is really what you want, type \"!reset yes\" to proceed."
                send_dm(convo['last_message']['sender']['did'], message)
            elif convo['last_message']['text'].lower() == "!reset yes":
                config = get_config()
                config['reply_settings'] = [entry for entry in config['reply_settings'] if entry['did'] != convo['last_message']['sender']['did']]
                config['follow_watches'] = [watch for watch in config['follow_watches'] if watch['did'] != convo['last_message']['sender']['did']]
                config['repost_defaults'] = [entry for entry in config['repost_defaults'] if entry['did'] != convo['last_message']['sender']['did']]
                config['user_watches'] = [watch for watch in config['user_watches'] if not (watch['receiver-handle'] == convo['last_message']['sender']['did'])]
                save_config(config)
                message = "All of your SkyAlert settings have been deleted. Thank you for using SkyAlert. If you want to use SkyAlert again, just enable follow watches or use the !watch command to watch someone."
                send_dm(convo['last_message']['sender']['did'], message)
                
# dangling cache check; if someone has disabled follow watching, remove their followers cache
def dangling_cache_check():
    if VERBOSE_PRINTING: print("Checking for dangling caches...")
    cached_files = [f for f in os.listdir(CACHE_DIR) if f.startswith("followers-") and f.endswith(".yaml")]
    valid_dids = {watch['did'] for watch in get_config().get('follow_watches', [])}
    
    for cached_file in cached_files:
        cached_did = cached_file[len("followers-"):-len(".yaml")]
        if cached_did not in valid_dids:
            if VERBOSE_PRINTING: print(f"Deleting dangling cache file: {cached_file}")
            os.remove(os.path.join(CACHE_DIR, cached_file))

# main logic
def main():
    # restart the post notifications module if it hasn't been responding for 10 minutes
    if firehose_check():
        os.system("systemctl restart skyalert-firehose")
    
    # verify user watch validity
    if VERBOSE_PRINTING: print("Checking user watches...")
    for watch in get_config().get('user_watches'):
        if VERBOSE_PRINTING: print(f"Checking watch for {watch.get('subject')} with receiver {watch.get('receiver')}...")
        if VERBOSE_PRINTING: print("Verifying DIDs...")
        receiver_did = watch.get('receiver-did')
        subject_did = watch.get('subject-did')
        receiver_handle = watch.get('receiver-handle')
        subject_handle = watch.get('subject-handle')
            
        if receiver_did == None or receiver_did == "":
            if VERBOSE_PRINTING: print("Invalid receiver, all watches for this receiver will be removed...")
            config = get_config()
            config['user_watches'] = [w for w in config['user_watches'] if w['receiver'] != watch.get('receiver')]
            save_config(config)
            continue
            
        if subject_did == None or subject_did == "":
            if VERBOSE_PRINTING: print("Invalid subject, the watch will be removed...")
            send_dm(receiver_did, f"You're no longer watching {watch.get('subject-handle')} because the handle is invalid.")
            config = get_config()
            config['user_watches'] = [w for w in config['user_watches'] if not (w['subject'] == watch.get('subject') and w['receiver'] == watch.get('receiver'))]
            save_config(config)
            continue
        
        try:
            subject_profile = client.get_profile(subject_did).model_dump()
            if subject_profile['handle'] != subject_handle:
                config = get_config()
                for watch in config['user_watches']:
                    if watch['subject-did'] == subject_did:
                        watch['subject-handle'] = subject_profile['handle']
                save_config(config)
        except atproto_client.exceptions.BadRequestError as e:
            if VERBOSE_PRINTING: print(f"Bad HTTP request when processing subject {subject_did}: {e}")
            config = get_config()
            receivers_to_notify = [
                watch['receiver-did'] for watch in config['user_watches'] if watch['subject-did'] == subject_did
            ]
            config['user_watches'] = [
                watch for watch in config['user_watches'] if watch['subject-did'] != subject_did
            ]
            save_config(config)
            for receiver_did in receivers_to_notify:
                send_dm(receiver_did, f"You are no longer watching {subject_handle} because the handle could not be verified. This usually happens when an account is deleted/deactivated by its user or suspended by Bluesky.")
            
        try:
            receiver_profile = client.get_profile(receiver_did).model_dump()
            if receiver_profile['handle'] != receiver_handle:
                config = get_config()
                for watch in config['user_watches']:
                    if watch['receiver-did'] == receiver_did:
                        watch['receiver-handle'] = receiver_profile['handle']
                save_config(config)
        except atproto_client.exceptions.BadRequestError as e:
            if VERBOSE_PRINTING: print(f"Exception occurred while processing receiver {receiver_did}: {e}")
            config = get_config()
            config['user_watches'] = [
            watch for watch in config['user_watches'] if watch['receiver-did'] != receiver_did
            ]
            save_config(config)
    
    # logic for follow watches (user is notified when someone unfollows them)
    # this does not need to be real-time, so it can run by polling
    if VERBOSE_PRINTING: print("Checking follow watches...")
    for user in get_config().get('follow_watches'):
        if VERBOSE_PRINTING: print(f"Checking watch for {user}...")
        if VERBOSE_PRINTING: print("Verifying DID...")
        user_did = user['did']
        user_handle = user['handle']
        
        if user_did == None or user_did == "":
            if VERBOSE_PRINTING: print("Invalid user, watch will be removed...")
            config = get_config()
            config['follow_watches'] = [w for w in config['follow_watches'] if w != user]
            save_config(config)
            continue
        
        try:
            client.get_profile(user_did)
        except atproto_client.exceptions.BadRequestError as e:
            if VERBOSE_PRINTING: print(f"Exception occurred while processing user {user_did}: {e}")
            config = get_config()
            config['follow_watches'] = [w for w in config['follow_watches'] if w['did'] != user_did]
            save_config(config)
            continue
        
        if VERBOSE_PRINTING: print("Loading cached followers...")
        cached_followers = []
        if not os.path.exists(f"{CACHE_DIR}/followers-{user_did}.yaml"):
            with open(f"{CACHE_DIR}/followers-{user_did}.yaml", 'w') as f:
                cached_followers = []
                yaml.dump(cached_followers, f)
        else:
            with open(f"{CACHE_DIR}/followers-{user_did}.yaml", 'r') as f:
                cached_followers = yaml.safe_load(f)
        
        if VERBOSE_PRINTING: print("Pulling current followers...")
        # Retrieve all current followers
        current_followers_objs = []
        current_followers_objs.append(client.get_followers(user_did).model_dump())
        while True:
            if "cursor" not in current_followers_objs[-1] or current_followers_objs[-1]["cursor"] is None:
                break
            current_followers_objs.append(client.get_followers(user_did, cursor=current_followers_objs[-1]['cursor']).model_dump())
        
        if VERBOSE_PRINTING: print("Getting DIDs of current followers...")
        # Separate the DIDs from the API objects
        current_followers_dids = []
        for obj in current_followers_objs:
            for follower in obj['followers']:
                current_followers_dids.append(follower['did'])
                
        if VERBOSE_PRINTING: print("Checking for unfollows...")
        # Check for unfollowers
        unfollowed_dids = []
        for cached_did in cached_followers:
            if cached_did not in current_followers_dids:
                unfollowed_dids.append(cached_did)
        
        if unfollowed_dids:
            message = "These users have unfollowed you:\n"
            profile_lines = []
            profile_fail = False
            for did in unfollowed_dids:
                try:    
                    profile = client.get_profile(did).model_dump()
                    profile_lines.append(f"- [{profile['handle']}](https://bsky.app/profile/{did})")
                except:
                    profile_lines.append(f"- {did}")
                    profile_fail = True
            
            message += "\n".join(profile_lines)
            if profile_fail: message += "\n\nSome profiles could not be loaded, so their handles are replaced by a DID. This usually happens when someone deletes their account or their account was suspended by the Bluesky team."
            send_dm(user_did, message)
        
        if VERBOSE_PRINTING: print("Saving follower cache...")
        # Update the cached followers list
        with open(f"{CACHE_DIR}/followers-{user_did}.yaml", 'w') as f:
            yaml.dump(current_followers_dids, f)
    
    # # last run time was only needed for user watching, so it is not needed anymore
    # if VERBOSE_PRINTING: print("Saving last run time...")
    # save_last_run()
    
@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=60),  # Exponential backoff
    stop=tenacity.stop_after_attempt(5),  # Stop after 5 attempts
    retry=tenacity.retry_if_exception_type(atproto_client.exceptions.RequestException)
)
def bot_commands_handler_with_retry():
    bot_commands_handler()

@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=60),  # Exponential backoff
    stop=tenacity.stop_after_attempt(5),  # Stop after 5 attempts
    retry=tenacity.retry_if_exception_type(atproto_client.exceptions.RequestException)
)
def main_with_retry():
    main()
    
time_waited = 0
cmd_check_interval = 30
main_interval = 3600
while True:
    if time_waited % cmd_check_interval == 0: # this script continues to handle bot commands
        bot_commands_handler_with_retry()
    if time_waited % main_interval == 0: # this only handles follow watches
        dangling_cache_check()
        main_with_retry()
        
    if time_waited == main_interval:
        time_waited = 0
        
    time.sleep(1)
    time_waited += 1