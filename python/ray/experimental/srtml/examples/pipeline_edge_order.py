from ray.experimental import serve
from ray.experimental.srtml import *
import time
import ray
'''
An example showing srtml pipeline preserves the order of edge insertions.
'''

serve.init()


model1_type = AbstractModelType(input_type=str,output_type=str,num_inputs=1)
model1 = AbstractModel(feature="echo",model_type=model1_type)

# model2_type = AbstractModelType(input_type=str,output_type=str,num_inputs=1)
model2 = AbstractModel(feature="echo",model_type=model1_type)

model3_type = AbstractModelType(input_type=str,output_type=str,num_inputs=2)
model3 = AbstractModel(feature="complex-echo",model_type=model3_type)



pipeline1 = Pipeline()
pipeline2 = Pipeline()

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
	f = pipeline2.remote("INP-{}".format(r),slo=slo)
	future_list.append(f)
left_futures = future_list
while left_futures:
	completed_futures , remaining_futures = ray.wait(left_futures,timeout=0.05)
	if len(completed_futures) > 0:
		result = ray.get(completed_futures)
		print("--------------------------------")
		print(result)
	left_futures = remaining_futures
