from ray.experimental import serve
from ray.experimental import srtml
import time

'''
An example showing error thrown because model3 excepts only one input. 
Lazy provisioning of pipeline helps in checking for checks.
'''

serve.init()


model1 = srtml.AbstractModel("echo",input_type=str,output_type=str,num_inputs=1)
model2 = srtml.AbstractModel("echo",input_type=str,output_type=str,num_inputs=1)
model3 = srtml.AbstractModel("complex-echo",input_type=str,output_type=str,num_inputs=1)

pipeline1 = srtml.Pipeline()
pipeline1.add_dependency(model1,model3)
pipeline1.add_dependency(model2,model3)
pipeline1.provision_pipeline()
