from ray.experimental import serve
from ray.experimental import srtml
import time

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

print("PIPELINE-1 ----------------------------------------------")
result = pipeline1.remote("INP")
print(result)
print("PIPELINE-2 ----------------------------------------------")
result = pipeline2.remote("INP")
print(result)