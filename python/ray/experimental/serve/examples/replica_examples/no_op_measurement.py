import ray
import queue
import time

@ray.remote
def simple_fun():
    return 1



def examine_futures(future_queue,timing_stats,num_q):
	pending_futures = []
	print("Started")
	c = 0
	while True:
		new_pending_futures = []	
		try:
		    item  = future_queue.get(block=True,timeout=0.00009)
		    new_pending_futures.append(item)
		    c += 1
		except Exception:
		    pass
		if len(pending_futures) == 0 and c == num_q:
			break
		pending_futures = pending_futures + new_pending_futures
		if len(pending_futures) == 0:
			continue
		completed_futures , remaining_futures = ray.wait(pending_futures,timeout=0.00001)
		if len(completed_futures) == 1:
			f = completed_futures[0]
			timing_stats[f] = time.time()
		pending_futures = remaining_futures
	print("ended")
	return

def send_queries(future_queue,associated_query,num_q):
	for _ in range(num_q):
		start_time = time.time()
		f = simple_fun.remote()
		future_queue.put_nowait(f)
		associated_query[f] = start_time

from concurrent.futures import ThreadPoolExecutor, wait, as_completed
associated_query = {}
timing_stats = {}
num_q = 1000

future_queue = queue.Queue()
f1 = pool.submit(send_queries,future_queue,associated_query,num_q)
f1 = pool.submit(examine_futures,future_queue,timing_stats,num_q)
wait([f1,f2])

for f in associated_query.keys():
	time_taken = timing_stats[f] - associated_query[f]
	print("OID: {}  Time Taken (in seconds): {}".format(f,time_taken))

