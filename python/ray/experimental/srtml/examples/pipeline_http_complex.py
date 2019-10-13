from ray.experimental import serve
from ray.experimental import srtml
import time
from ray.experimental.serve.utils import pformat_color_json
import requests
'''
A complex pipeline example with HTTP!
'''

serve.init()
model1 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=1)

model2 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=1)
model3 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=1)

model4 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=2)
model5 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=3)
model6 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=1)
model7 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=1)

model8 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=2)
model9 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=2)

model10 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=2)


pipeline = srtml.Pipeline()

pipeline.add_dependency(model1,model2)
pipeline.add_dependency(model1,model3)
pipeline.add_dependency(model1,model5)

pipeline.add_dependency(model2,model4)
pipeline.add_dependency(model2,model5)

pipeline.add_dependency(model3,model6)
pipeline.add_dependency(model3,model7)

pipeline.add_dependency(model4,model8)

pipeline.add_dependency(model5,model9)

pipeline.add_dependency(model6,model5)
pipeline.add_dependency(model6,model8)

pipeline.add_dependency(model7,model4)
pipeline.add_dependency(model7,model9)

pipeline.add_dependency(model8,model10)
pipeline.add_dependency(model9,model10)




pipeline.provision_pipeline()
http_address = pipeline.http()
time.sleep(2)
# result = pipeline.remote("INP")
# print(result)
while True:
	data = "INP"
	data = pipeline.get_http_formatted_data(data)
	resp = requests.post(http_address,data = data).json()
	print(pformat_color_json(resp))

	print("...Sleeping for 2 seconds...")
	time.sleep(2)
