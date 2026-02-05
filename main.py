from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel
from .datastore import RedisKeyVal
import datetime
import asyncio
import os


class SlotDocument(BaseModel):
    type: str
    content: str
    skey: str

class Slot():
    def __init__(self, uid, default_data):
        self.lock = asyncio.Lock()
        self.store = RedisKeyVal(uid, default_data)


APP_SECRET = "insecure_placeholder_string"
try:
    APP_SECRET = os.environ['APP_SECRET']
except:
    pass

MAX_SLOT_ITEMS = 100

empty_logs = [{} for _ in range(MAX_SLOT_ITEMS)]
default_slot_data = {"cur_index": 0, "content":{"type":"text/plain","data":""}, "logs": empty_logs}

slotnums = ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"]

slots_kvstores = {}
for slot_id in slotnums:
    slots_kvstores[slot_id] = Slot(slot_id, default_slot_data)

def get_slot_kvstore(slotnum):
    return slots_kvstores[slotnum]

async def request_to_json(r):
    method = r.method
    path = r.url.path
    # http_version is stored in the ASGI 'scope' dictionary
    version = r.scope.get("http_version")

    request_line = f"{method} {path} HTTP/{version}"
    body_bytes = await r.body()
    # Convert bytes to string
    body_str = body_bytes.decode("utf-8")
    return {
        "req_line": request_line,
        "host": r.client.host if r.client else None,
        "port": r.client.port if r.client else None,
        "headers": dict(r.headers),
        "query_params": dict(r.query_params),
        "body_data": body_str
    }


app = FastAPI()

@app.get("/webhook_ui")
async def get_ui_html():
    return FileResponse('static/index.html')

@app.get("/slot_logs/{slotnum}")
async def get_slot_logs(slotnum: str):
    if slotnum not in slotnums:
        return None
    # query database for log entries matching slotnum
    slot = get_slot_kvstore(slotnum)
    rval = {}
    async with slot.lock:
        rval = await slot.store.get()
    return rval

@app.get("/s/{slotnum}")
@app.post("/s/{slotnum}")
@app.put("/s/{slotnum}")
@app.patch("/s/{slotnum}")
@app.delete("/s/{slotnum}")
async def get_slot_doc(slotnum: str, request: Request, response: Response):
    rval = None
    if slotnum in slotnums:
        now = str(datetime.datetime.now())
        new_log_entry = {"timestamp": now, "request": await request_to_json(request)}

        slot = get_slot_kvstore(slotnum)
        async with slot.lock:
            slot_data = await slot.store.get()
            next_index = (slot_data['cur_index'] + 1) % MAX_SLOT_ITEMS
            slot_data['logs'][next_index] = new_log_entry
            slot_data['cur_index'] = next_index
            await slot.store.set(slot_data)
            rval = Response(content=slot_data['content']['data'], media_type=slot_data['content']['type'])

    return rval

@app.post("/slot_content/{slotnum}")
async def set_doc(slotnum: str, doc: SlotDocument):
    if doc.skey != APP_SECRET:
        return None
    # query database for content for slotnum
    # update db
    slot = get_slot_kvstore(slotnum)
    async with slot.lock:
        slot_data = await slot.store.get()
        slot_data['content']['type'] = doc.type
        slot_data['content']['data'] = doc.content
        await slot.store.set(slot_data)
    return slot_data['content']
