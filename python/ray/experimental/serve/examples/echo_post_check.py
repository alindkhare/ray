import time

import requests
from werkzeug import urls

from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json
import json

def echo1(context):
	message = ""
	message += 'FROM MODEL1 -> '
	return message
def identity(context):
	return context['serve1']
# def echo2(context):
# 	data_from_service1 = context['serve1']
# 	data_from_service1 += 'FROM MODEL2 -> '
# 	return data_from_service1

# def echo3(context):
# 	data_from_service2 = context['serve2']
# 	data_from_service2 += 'FROM MODEL3 -> '
# 	return data_from_service2

serve.init(blocking=True)

# serve.create_endpoint_pipeline("pipeline1", "/echo", blocking=True)

# Create Backends
serve.create_backend(echo1, "echo:v1")
serve.create_backend(identity, "identity:b")
# serve.create_backend(echo2, "echo:v2")
# serve.create_backend(echo3,"echo:v3")

# Create services
serve.create_no_http_service("serve1")
serve.create_no_http_service("identity")
# serve.create_no_http_service("serve2")
# serve.create_no_http_service("serve3")

# Link services and backends
serve.link_service("serve1", "echo:v1")
serve.link_service("identity", "identity:b")
# serve.link_service("serve2", "echo:v2")
# serve.link_service("serve3","echo:v3")

'''
1. Add service dependencies in a PIPELINE
2. You can add dependency to a PIPELINE only if the PIPELINE has not been provisioned yet.
'''
'''
Creating a pipeline serve1 -> serve2 -> serve3
'''
# serve2 depends on serve1
serve.add_service_dependencies("pipeline1","serve1","identity")
# serve3 depends on serve2
# serve.add_service_dependencies("pipeline1","serve2","serve3")

# Provision the PIPELINE (You can provision the pipeline only once)
serve.provision_pipeline("pipeline1")

# You can only create an endpoint for pipeline after provisioning the pipeline
serve.create_endpoint_pipeline("pipeline1", "/echo", blocking=True)

time.sleep(2)

data = {'data':[1,2,3,6], 'model': 'resnet'}
while True:
    resp = requests.post("http://127.0.0.1:8000/echo",data = data).json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)