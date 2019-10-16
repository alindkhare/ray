from ray.experimental import serve
from ray.experimental import srtml
import time
import ray
'''
An example showing srtml pipeline preserves the order of edge insertions.
'''

serve.init()


model1 = srtml.AbstractModel("echo",input_type=str,output_type=str,num_inputs=1)
model2 = srtml.AbstractModel("echo",input_type=str,output_type=str,num_inputs=1)
model3 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=2)



pipeline1 = srtml.Pipeline()
pipeline2 = srtml.Pipeline()

pipeline1.add_dependency(model1,model3)
pipeline1.add_dependency(model2,model3)
pipeline1.provision_pipeline()

pipeline2.add_dependency(model2,model3)
pipeline2.add_dependency(model1,model3)
pipeline2.provision_pipeline()



time.sleep(1)
print("Model1 : {} Model2: {}".format(model1.get_backend(),model2.get_backend()))


print("PIPELINE-1 ##################################################################################")
future_list = []
for r in range(10):
	slo = 1000 + 100*r
	f = pipeline1.remote("INP-{}".format(r),slo=slo)
	future_list.append(f)
left_futures = future_list
while left_futures:
	completed_futures , remaining_futures = ray.wait(left_futures,timeout=0.05)
	if len(completed_futures) > 0:
		result = ray.get(completed_futures)
		print("--------------------------------")
		print(result)
	left_futures = remaining_futures

# print("PIPELINE-1 ----------------------------------------------")
# f = pipeline1.remote("INP")
# result = ray.get(f)
# print(result)
# f = pipeline2.remote("INP")
# result = ray.get(f)
# print(result)
print("PIPELINE-2 ###################################################################################")
future_list = []
for r in range(10):
	slo = 1000 - 100*r
	f = pipeline2.remote("INP-{}".format(r)e,slo=slo)
	future_list.append(f)
left_futures = future_list
while left_futures:
	completed_futures , remaining_futures = ray.wait(left_futures,timeout=0.05)
	if len(completed_futures) > 0:
		result = ray.get(completed_futures)
		print("--------------------------------")
		print(result)
	left_futures = remaining_futures
