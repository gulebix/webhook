import os
import json
import asyncio
import hashlib
import redis
# pip install redis
# conda install redis-py

REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])
FILE_CACHE_DIR = 'tmp'

def hash_key(key: str):
    encoded_string = key.encode('utf-8')
    hash_object = hashlib.sha256(encoded_string)
    hex_digest = hash_object.hexdigest()
    return f"{hex_digest}"


class RedisKeyVal():
    def __init__(self, uid: str, default_data = {}):
        self.key = hash_key(uid)
        self._mutex = asyncio.Lock()
        self.default_data = default_data

    def filesystem_cache_path(self):
        return f'/{FILE_CACHE_DIR}/{self.key}'

    def get_redis_connection(self):
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        return r

    async def get(self):
        val = None
        await self._mutex.acquire()
        try:
            r = self.get_redis_connection()
            try:
                rawval = r.get(self.key)
                val = json.loads(rawval)
                with open(self.filesystem_cache_path(), 'w') as jsonfile:
                    jsonfile.write(rawval)
            except redis.ConnectionError as ce:
                pass
            except TypeError as e:
                print(f"{type(e).__name__} | {e}")
                try:
                    with open(self.filesystem_cache_path(), 'r') as jsonfile:
                        val = json.load(jsonfile)
                        r.set(self.key, json.dumps(val))
                except Exception as e:
                    print(f"{type(e).__name__} | {e}")
                    tmp_jsonstr = json.dumps(self.default_data)
                    r.set(self.key, tmp_jsonstr)
                    with open(self.filesystem_cache_path(), 'w') as jsonfile:
                        jsonfile.write(tmp_jsonstr)
                    val = self.default_data
            r.close()
        except Exception as e:
            print(f"{type(e).__name__} | {e}")
        finally:
            self._mutex.release()
        return val

    async def set(self, val):
        val = json.dumps(val)
        await self._mutex.acquire()
        try:
            r = self.get_redis_connection()
            r.set(self.key, val)
            r.close()
            with open(self.filesystem_cache_path(), 'w') as jsonfile:
                jsonfile.write(val)
        except Exception as e:
            print(f"{type(e).__name__} | {e}")
        finally:
            self._mutex.release()
        return val


# Connect to your Key Value instance using the REDIS_URL environment variable
# The REDIS_URL is set to the internal connection URL e.g. redis://red-343245ndffg023:6379
#r = redis.from_url(os.environ['REDIS_URL'])

'''

data exists in/on:
|filesystem|redis|outcome                                    |
|    -     |  -  |  -                                        |
|    Y     |  Y  |fetch from redis, write to filesystem cache|
|    Y     |  N  |restore filesystem to redis                |
|    N     |  Y  |fetch from redis, write to filesystem cache|
|    N     |  N  |create new redis data                      |

MUTEX_LOCK
- connect to redis
- fetch data
- if no data:
    - if filesystem backup:
        - load filesystem backup
        - restore data to redis
    - else:
        - create new redis data struct on filesystem
        - restore to redis
    - return the filesystem data
- else:
    - backup to filesystem
MUTEX_UNLOCK

**process the data or return it

MUTEX_LOCK
- connect to redis
- save data
- backup to filesystem
MUTEX_UNLOCK
'''
