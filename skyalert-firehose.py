import multiprocessing
import signal
import time
from collections import defaultdict
from types import FrameType
from typing import Any
from atproto import CAR, AtUri, FirehoseSubscribeReposClient, firehose_models, models, parse_subscribe_repos_message, Client, IdResolver, SessionEvent
import atproto_client
import json
import atproto_client.exceptions
import yaml
import os
import datetime
import re
import tenacity

# code from original skyalert file, now skyalert-cmds.py

# global variables
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
CONFIG_FILE = os.path.join(DATA_DIR, 'config.yaml')
CACHE_DIR = os.path.join(DATA_DIR, 'cache')
LAST_RUN_FILE = os.path.join(DATA_DIR, 'last_run.txt')
VERBOSE_PRINTING = False

global client
client = Client()

@client.on_session_change
def on_session_change(event: atproto_client.SessionEvent,session: atproto_client.Session):
    if event==SessionEvent.CREATE or event==SessionEvent.REFRESH:
        dm_client = client.with_bsky_chat_proxy()
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
        dm_client = client.with_bsky_chat_proxy()
    else:
        client.login(login=login_info['username'], password=login_info['password'])
        dm_client = client.with_bsky_chat_proxy()

# Link detection by latchk3y on the Bluesky API Discord server
def get_facets(text):
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

    return facets

def get_config():
    if not os.path.exists(CONFIG_FILE):
        return {'user_watches': [], 'follow_watches': []}
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)
        return config
    
def get_followers_cache(did):
    cache_file = os.path.join(CACHE_DIR, f'followers-{did}.json')
    if not os.path.exists(cache_file):
        return []
    with open(cache_file, 'r') as f:
        return json.load(f)
    
def save_followers_cache(did, new_cache):
    cache_file = os.path.join(CACHE_DIR, f'followers-{did}.json')
    with open(cache_file, 'w') as f:
        json.dump(new_cache, f)
    
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
    
def post_url_from_at_uri(at_uri):
    # Split the AT URI to extract the DID and the random string
    parts = at_uri.split('/')
    did = parts[2]
    random_string = parts[-1]
    
    # Construct the Bluesky URL
    url = f"https://bsky.app/profile/{did}/post/{random_string}"
    return url

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
    
    # send a message to the conversation
    dm.send_message(
        models.ChatBskyConvoSendMessage.Data(
            convo_id=convo.id,
            message=models.ChatBskyConvoDefs.MessageInput(
                text=message,
                facets=get_facets(message)
            ),
        )
    )

    if VERBOSE_PRINTING: print('\nMessage sent!')

# from the atproto python repo examples

_INTERESTED_RECORDS = {
    models.ids.AppBskyFeedPost: models.AppBskyFeedPost,
    # models.ids.AppBskyGraphFollow: models.AppBskyGraphFollow, # the bot doesn't need to see follows for the forseeable future
    models.ids.AppBskyFeedRepost: models.AppBskyFeedRepost
}

def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> defaultdict:
    operation_by_type = defaultdict(lambda: {'created': [], 'deleted': []})

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        if op.action == 'update':
            # not supported yet
            continue

        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

        if op.action == 'create':
            if not op.cid:
                continue

            create_info = {'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo}

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = models.get_or_create(record_raw_data, strict=False)
            record_type = _INTERESTED_RECORDS.get(uri.collection)
            if record_type and models.is_record_type(record, record_type):
                operation_by_type[uri.collection]['created'].append({'record': record, **create_info})

        if op.action == 'delete':
            operation_by_type[uri.collection]['deleted'].append({'uri': str(uri)})

    return operation_by_type


def worker_main(cursor_value: multiprocessing.Value, pool_queue: multiprocessing.Queue) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # we handle it in the main process

    while True:
        message = pool_queue.get()

        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            continue

        if commit.seq % 20 == 0:
            cursor_value.value = commit.seq

        if not commit.blocks:
            continue

        ops = _get_ops_by_type(commit)
        for created_post in ops[models.ids.AppBskyFeedPost]['created']:
            for watch in get_config()['user_watches']:
                if watch['subject-did'] == created_post['author']:
                    post = created_post['record']
                    profile = client.get_profile(created_post['author'])
                    post_url = post_url_from_at_uri(created_post['uri'])
                    message1 = f"{profile.handle} said: {post['text'].replace("\n", " ")}"
                    message2 = f"Link to post: {post_url}"
                    send_dm(watch['receiver-did'], message1)
                    send_dm(watch['receiver-did'], message2)

        # # The followers cache is disabled; it was required only for follow watching.
        # for created_follow in ops[models.ids.AppBskyGraphFollow]['created']:
        #     author = created_follow['author']
        #     record = created_follow['record']
        #     uri = created_follow['uri']
            
        #     for watch in get_config()['follow_watches']:
        #         if watch['did'] == record['subject']:
        #             followers_cache = get_followers_cache(record['subject'])
        #             followers_cache.append(author)
        #             save_followers_cache(record['subject'], followers_cache)
                    
        for created_repost in ops[models.ids.AppBskyFeedRepost]['created']:
            for watch in get_config()['user_watches']:
                if watch['subject-did'] == created_repost['author'] and watch['reposts-allowed']:
                    post = created_repost['record']
                    reposter_handle = watch['subject-handle']
                    reposted_profile = client.get_profile(post['subject'].uri.split('/')[2])
                    post_url = post_url_from_at_uri(post['subject'].uri)
                    message = f"{reposter_handle} reposted {reposted_profile.handle}\n{post_url}"
                    send_dm(watch['receiver-did'], message)
        
        # Deleted follow logic is not implemented yet; the unfollower can be found but not the person who was unfollowed
                    
        
def get_firehose_params(cursor_value: multiprocessing.Value) -> models.ComAtprotoSyncSubscribeRepos.Params:
    return models.ComAtprotoSyncSubscribeRepos.Params(cursor=cursor_value.value)


def measure_events_per_second(func: callable) -> callable:
    def wrapper(*args) -> Any:
        wrapper.calls += 1
        cur_time = time.time()

        if cur_time - wrapper.start_time >= 1:
            #print(f'NETWORK LOAD: {wrapper.calls} events/second')
            wrapper.start_time = cur_time
            wrapper.calls = 0

        return func(*args)

    wrapper.calls = 0
    wrapper.start_time = time.time()

    return wrapper


def signal_handler(_: int, __: FrameType) -> None:
    print('Keyboard interrupt received. Waiting for the queue to empty before terminating processes...')

    # Stop receiving new messages
    firehose.stop()

    # Drain the messages queue
    while not queue.empty():
        #print('Waiting for the queue to empty...')
        time.sleep(0.2)

    #print('Queue is empty. Gracefully terminating processes...')

    pool.terminate()
    pool.join()

    exit(0)

if __name__ == '__main__':
    global firehose
    
    signal.signal(signal.SIGINT, signal_handler)

    start_cursor = None

    params = None
    cursor = multiprocessing.Value('i', 0)
    if start_cursor is not None:
        cursor = multiprocessing.Value('i', start_cursor)
        params = get_firehose_params(cursor)

    firehose = FirehoseSubscribeReposClient(params)

    workers_count = multiprocessing.cpu_count() * 2 - 1
    max_queue_size = 10000

    queue = multiprocessing.Queue(maxsize=max_queue_size)
    pool = multiprocessing.Pool(workers_count, worker_main, (cursor, queue))
    
    @measure_events_per_second
    def on_message_handler(message: firehose_models.MessageFrame) -> None:
        if cursor.value:
            # we are using updating the cursor state here because of multiprocessing
            # typically you can call client.update_params() directly on commit processing
            firehose.update_params(get_firehose_params(cursor))

        queue.put(message)

    firehose.start(on_message_handler)