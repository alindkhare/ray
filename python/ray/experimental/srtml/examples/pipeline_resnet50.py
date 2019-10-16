from ray.experimental import serve
from ray.experimental import srtml
import time
import torch
import base64
import requests
from ray.experimental.serve.utils import pformat_color_json
import ray

'''
An example showing error thrown because model3 excepts only one input. 
Lazy provisioning of pipeline helps in checking for checks.
'''

serve.init(object_store_memory=int(1e9))

transform_model = srtml.AbstractModel("imagenet-transform",output_shape=(3,224,224),input_type=str,output_type=torch.Tensor,num_inputs=1)
resnet_model = srtml.AbstractModel("imagenet-resnet",input_shape=(3,224,224),input_type=torch.Tensor,output_type=int,num_inputs=1)

classification_p = srtml.Pipeline()
classification_p.add_dependency(transform_model,resnet_model)

classification_p.provision_pipeline()
# http_address = classification_p.http()
# time.sleep(2)

future_list = []
for r in range(10):
	slo = 1000 + 100*r
	data = base64.b64encode(open('elephant.jpg', "rb").read())
	f = classification_p.remote(data,slo=slo)
	future_list.append(f)
left_futures = future_list
while left_futures:
	completed_futures , remaining_futures = ray.wait(left_futures,timeout=0.05)
	if len(completed_futures) > 0:
		result = ray.get(completed_futures)
		print("--------------------------------")
		print(result)
	left_futures = remaining_futures

