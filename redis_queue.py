import redis
import pprint
import time
import json
import uuid
import os
import dotenv

dotenv.load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
class Queue:
    def __init__(
        self, queue_name, redis_url, status_name="job_status", result_name="job_result", password=os.getenv("REDIS_PASSWORD", None)
    ):
        self.redis_client = redis.Redis.from_url(redis_url, password=password)
        self.queue_name = queue_name
        self.status_name = status_name
        self.result_name = result_name

    def enqueue(self, job, ttl=None):
        job_id = str(uuid.uuid4())[:8]
        job["id"] = job_id
        serialized_job = json.dumps(job)
        job_key = f"{self.queue_name}:job:{job['id']}"
        self.redis_client.set(job_key, serialized_job)
        if ttl is not None:
            self.redis_client.expire(job_key, ttl)
        self.redis_client.lpush(self.queue_name, job_key)
        self.redis_client.hset(self.status_name, job['id'], 'queued')
        return job_id

    def dequeue(self):
        _, job_key = self.redis_client.brpop(self.queue_name)
        serialized_job = self.redis_client.get(job_key)
        
        if serialized_job is None:
            print("job with key {} expired".format(job_key))
            self.redis_client.lrem(self.queue_name, 0, job_key)
            return None
        
        job = json.loads(serialized_job)
        job_status = self.redis_client.hget(self.status_name, job['id'])

        if job_status != b'cancelled':
            self.redis_client.hset(self.status_name, job['id'], 'processing')
            return job

        # Job marked for removal, so we just skip it
        print("job with key {} was cancelled".format(job_key))
        self.redis_client.hdel(self.status_name, job['id'])
        self.redis_client.lrem(self.queue_name, 0, job_key)
        return None


    def set_failed(self, job_id):
        self.redis_client.hset(self.status_name, job_id, "failed")

    def get_status(self, job_id):
        return self.redis_client.hget(self.status_name, job_id).decode()

    def get_result(self, job_id):
        serialized_result = self.redis_client.hget(self.result_name, job_id)
        if not serialized_result:
            print("failed")
            self.set_failed(job_id)
            return None
        return json.loads(serialized_result)

    def is_empty(self):
        return self.redis_client.llen(self.queue_name) == 0

    def complete(self, job_id, result):
        serialized_result = json.dumps(result)
        self.redis_client.hset(self.result_name, job_id, serialized_result)
        self.redis_client.hset(self.status_name, job_id, "completed")

    def is_empty(self):
        return self.redis_client.llen(self.queue_name) == 0

    def length(self):
        return self.redis_client.llen(self.queue_name)

    def delete(self):
        self.redis_client.delete(self.queue_name)

    def peek_jobs(self, start=0, end=-1):
        serialized_jobs = self.redis_client.lrange(self.queue_name, start, end)
        jobs = [json.loads(serialized_job) for serialized_job in serialized_jobs]
        return jobs[::-1]
    def remove_job(self, job_id):
        self.redis_client.hset(self.status_name, job_id, 'cancel')
        # serialized_jobs = self.redis_client.lrange(self.queue_name, 0, -1)
        # for serialized_job in serialized_jobs:
        #     job = json.loads(serialized_job)
        #     if job['id'] == job_id:
        #         self.redis_client.lrem(self.queue_name, 0, serialized_job)
        #         print(f"Removed job {job_id} from the queue")
        #         print("serialized_job", pprint.pformat(serialized_job))
        #         return True
        # print(f"Job {job_id} not found in the queue")
        # return False
        
# def example_job_handler(job):
#     print(f"Processing job: {job}")
#     time.sleep(1)
#     result = {"result": "success", "data": job['task'].upper()}
#     print(f"Finished processing job: {job}")
#     return result


# import concurrent.futures

# if __name__ == "__main__":
#     consumer = JobConsumer()

#     # Process jobs
#     while not consumer.is_empty():
#         job = consumer.dequeue()
#         result = example_job_handler(job)
#         consumer.complete(job['id'], result)


# # Example usage:

# def example_job_handler(job):
#     print(f"Processing job: {job}")
#     time.sleep(1)
#     print(f"Finished processing job: {job}")

if __name__ == "__main__":
    job_queue = Queue("test", REDIS_URL)

    params = {
        "command": "stable",
        "model_id": "illum", 
        "num_generations": 2,
        # "prompt": prompts[i % len(prompts)],
        "prompt":"puppy"
    }
    # Enqueue jobs and store their IDs
    job_ids = []
    for i in range(1):
        t = time.time()
        job_id = job_queue.enqueue(params)
        print(job_id)
        job_ids.append(job_id)
        job_status = job_queue.get_status(job_id)
        while True and job_status == "queued" or job_status == "processing":
            job_status = job_queue.get_status(job_id)
            # time.sleep(.)
        print(f"job_status = {job_status}")
        job_result = job_queue.get_result(job_id)
        print("time to complete: ", time.time() - t)
        print(f"job_result = {job_result}")
    print(f"{job_ids}")

    # Process jobs
    # while not job_queue.is_empty():
    #     job = job_queue.dequeue()
    #     example_job_handler(job)
    #     job_queue.complete(job['id'])

    # # Print job statuses
    # for job_id in job_ids:
    #     status = job_queue.get_status(job_id)
    #     print(f"Job {job_id} status: {status}")
