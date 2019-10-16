from ray.experimental import serve
from ray.experimental.srtml import *
import ray
'''
A hello world example for pipeline provisioning. 
'''


serve.init()

model_type = AbstractModelType(input_type=str,output_type=str,num_inputs=1)
model1 = AbstractModel(feature="echo",model_type=model_type)
model2 = AbstractModel(feature="complex-echo",model_type=model_type)

pipeline = Pipeline()
pipeline.add_dependency(model1,model2)
pipeline.provision_pipeline()
future_list = []
for r in range(10):
	slo = 1000 + 100*r
	f = pipeline.remote("INP-{}".format(r),slo=slo)
	future_list.append(f)
left_futures = future_list
while left_futures:
	completed_futures , remaining_futures = ray.wait(left_futures,timeout=0.05)
	if len(completed_futures) > 0:
		result = ray.get(completed_futures)
		print("--------------------------------")
		print(result)
	left_futures = remaining_futures
# result = pipeline.remote("INP")
# print(result)