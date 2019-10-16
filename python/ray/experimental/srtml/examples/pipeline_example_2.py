from ray.experimental import serve
from ray.experimental import srtml
import time
import ray


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
# time.sleep(1)
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