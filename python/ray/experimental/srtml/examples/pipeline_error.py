from ray.experimental import serve
from ray.experimental.srtml import *
import time

'''
An example showing error thrown because model3 excepts only one input. 
Lazy provisioning of pipeline helps in checking for checks.
'''

serve.init()

model_type = AbstractModelType(input_type=str,output_type=str,num_inputs=1)
model1 = AbstractModel(feature="echo",model_type=model_type)
model2 = AbstractModel(feature="echo",model_type=model_type)
model3 = AbstractModel(feature="complex-echo",model_type=model_type)

pipeline1 = Pipeline()
pipeline1.add_dependency(model1,model3)
pipeline1.add_dependency(model2,model3)
pipeline1.provision_pipeline()
