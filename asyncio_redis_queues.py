import json
import traceback
import os
import uuid
import time
import dotenv
import asyncio
import redis.asyncio as redis
from typing import Union, Dict, Callable, List

dotenv.load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
CHANNEL_NAME = "activity"

JOB_DATA_NAME = "job_data"
JOB_STATUS_NAME = "job_status"

class Job:
    def __init__(self, job_id, queue: 'Queue'):
        self.queue = queue
        self.id = job_id
    
    async def data(self):
        serialized_job = await self.queue.redis_client.hget("jobs", self.id)
        return json.loads(serialized_job)
    
    async def set_status(self, status):
        return await self.queue.redis_client.hset(JOB_STATUS_NAME, self.id, json.dumps(status))

    async def get_status(self):
        return await self.queue.redis_client.hget(JOB_STATUS_NAME, self.id)

    async def notify(self, payload, status):
        pipe = self.queue.redis_client.pipeline()
        pipe.hset(JOB_STATUS_NAME, self.id, json.dumps(status))
        pipe.publish(CHANNEL_NAME, json.dumps({
            "job_id": self.id,
            "payload": payload
        }))
        await pipe.execute()

    def __repr__(self) -> str:
        return f"Job(id: {self.id}, queue: {self.queue})"

class MessageBox:
    _INSTANCE: Union['MessageBox', None] = None

    @staticmethod
    def get_instance(redis_client):
        if MessageBox._INSTANCE is None:
            MessageBox._INSTANCE = MessageBox(redis_client)
        return MessageBox._INSTANCE

    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
        self.listeners: Dict[str, asyncio.Future] = {}
        self.loop_task = asyncio.get_event_loop().create_task(self.loop())
        self.callbacks: List[Callable[[object]]] = []

    async def loop(self):
        self.pubsub = self.redis_client.pubsub()
        await self.pubsub.subscribe(CHANNEL_NAME)
        while asyncio.get_event_loop().is_running():
            try:
                message = await self.pubsub.get_message(ignore_subscribe_messages=True)
                if message is None: continue
                data = message["data"].decode("utf-8")
                data = json.loads(data)
                for callback in self.callbacks:
                    callback(data["job_id"], data["payload"])
                if data["job_id"] in self.listeners:
                    job_id = data["job_id"]
                    self.listeners[job_id].set_result(data["payload"])
                    del self.listeners[job_id]
            except:
                traceback.print_exc()
    
async def wait_for_job_update(job_id: str, redis_client):
    """
        Wait for updates on a job, and return them when they arrive.
        Currently, it is the caller's responsibility to re-wait for
        new job data if the job is not yet complete.

        Additional note, this function is not thread-safe and has to be used
        from the same asyncio event loop that it was first called from.
    """
    
    message_box = MessageBox.get_instance(redis_client)
    try:
        future = asyncio.get_event_loop().create_future()
        message_box.listeners[job_id] = future
        return await future
    except asyncio.CancelledError:
        if job_id in message_box.listeners:
            del message_box.listeners[job_id]
        raise

class Queue:
    def __init__(
        self,
        queue_name,
        redis_client: Union[redis.Redis, None] = None,
        redis_url: Union[str, None] = None,
        password: Union[str, None] = os.getenv("REDIS_PASSWORD", None)
    ):
        if redis_client:
            self.redis_client = redis_client
        elif redis_url:
            print("WARNING: inefficient redis queue usage, redis client should be reused")
            self.redis_client = redis.Redis.from_url(redis_url, password=password)
        else:
            raise ValueError("Either redis_client or redis_url must be provided")
        self.name = queue_name
        self.last_len = 0
    
    async def enqueue(self, job_data, priority: int = 60000):
        """Add a job to the queue, where priority is the number of milliseconds of advantage to give the job"""
        job_id = str(uuid.uuid4())
        priority = int(round(time.time() * 1000)) - priority
        print(f"priority = {priority}")
        await self.redis_client.hset("jobs", job_id, json.dumps(job_data))
        await asyncio.gather(
            self.redis_client.zadd(self.name, {job_id: priority}),
            Job(job_id, self).set_status("queued")
        )
        print('getting future')
        future = wait_for_job_update(job_id, self.redis_client)
        return future, job_id

    async def dequeue(self, timeout: int = 0):
        result = await self.redis_client.bzpopmin(self.name, timeout=timeout)
        if result:
            _, job_id, _ = result
            return Job(job_id.decode(), self)
        return None

    async def length(self):
        try:
            return await self.redis_client.zcard(self.name)
        except:
            return 0

    async def remove_job(self, job_id):
        await self.redis_client.zrem(self.name, job_id)
    
    async def get_job_position(self, job_id):
        return await self.redis_client.zrank(self.name, job_id) 

    def __repr__(self) -> str:
        return f"Queue({self.name}, client: {self.redis_client})"
   