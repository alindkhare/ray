from ray.experimental import serve
from ray.experimental import srtml

'''
A hello world example for pipeline provisioning. 
'''


serve.init()
model1 = srtml.AbstractModel("echo",input_type=str,output_type=str,num_inputs=1)
model2 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str)
pipeline = srtml.Pipeline()
pipeline.add_dependency(model1,model2)
pipeline.provision_pipeline()
result = pipeline.remote("INP")
print(result)