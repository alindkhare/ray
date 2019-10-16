import uuid
from ray.experimental import serve
from collections import defaultdict, deque
from ray.experimental.serve.utils import BytesEncoder
import time
import json

class Pipeline:
	def __init__(self):
		self.abstract_models_obj = {}
		self.abstract_models_config = defaultdict(dict)
		self.pipeline_name = uuid.uuid1().hex
		self.inverted_service_dependency = defaultdict(list)
		self.provisioned = False
		self.pipeline_info = None
		self.pipeline_handle = None
		self.http_served = False
	def fill_config(self,model):
		if not self.provisioned:
			config = model.get_config()
			self.abstract_models_config[model.service_name] = config

	def add_model(self,model):
		if not self.provisioned:
			if model.service_name not in self.abstract_models_obj:
				self.abstract_models_obj[model.service_name] = model
				self.fill_config(model)

	def __isWildCard(self,expr):
		return expr == '?'
	def __addSanityCheck(self,model1_sname,model2_sname):
		if not self.provisioned:
			final_check = True
			m1_config = self.abstract_models_config[model1_sname]
			m2_config = self.abstract_models_config[model2_sname]

			if self.__isWildCard(m2_config['num_inputs']) or m2_config['num_inputs'] > 0:
				final_check = True
			else:
				return False,"Cannot add more incoming edges!"
			if (m1_config['output_type'] == m2_config['input_type']) or self.__isWildCard(m1_config['output_type']) or self.__isWildCard(m2_config['input_type']):
				final_check = True
			else:
				return False,"Types do not match!"
			if (m1_config['output_shape'] == m2_config['input_shape'] or self.__isWildCard(m1_config['output_shape']) or  self.__isWildCard(m2_config['input_shape'])) :
				final_check = True
			else:
				return False,"Shape do not match!"

			return True,""



	def add_dependency(self,model1,model2):
		if not self.provisioned:
			self.add_model(model1)
			self.add_model(model2)
			flag,message = self.__addSanityCheck(model1.service_name,model2.service_name)
			if flag:
				self.inverted_service_dependency[model2.service_name].append(model1.service_name)
				if not self.__isWildCard(self.abstract_models_config[model2.service_name]['num_inputs']):
					self.abstract_models_config[model2.service_name]['num_inputs'] -= 1
			else:
				raise Exception(message)

	def provision_pipeline(self):
		if not self.provisioned:
			for abstract_model in self.abstract_models_obj.keys():
				model = self.abstract_models_obj[abstract_model]
				if not model.is_linked:
					model.link_model(max_batch_size=4)
			for service1 in self.inverted_service_dependency.keys():
				directed_edges = self.inverted_service_dependency[service1]
				for service2 in directed_edges:
					serve.add_service_dependencies(self.pipeline_name,service2,service1)
			serve.provision_pipeline(self.pipeline_name)
			time.sleep(1)
			self.pipeline_info = serve.get_service_dependencies(self.pipeline_name)
			self.pipeline_handle = serve.get_handle(self.pipeline_name)
			self.provisioned = True

	def remote(self,data,slo=None):
		if self.provisioned:
			node_list = self.pipeline_info['node_order'][0]
			sent = {}
			for n in node_list:
				sent[n] = data
			if slo is not None:
				sent['slo'] = slo
			return self.pipeline_handle.remote(**sent)
	def http(self,route=None):
		if self.provisioned and not self.http_served:
			if route is None:
				route = '/{}'.format(self.pipeline_name)
			serve.create_endpoint_pipeline(self.pipeline_name, route, blocking=True)
			self.http_served = True
			address = serve.global_state.http_address + route
			return address

	def get_http_formatted_data(self,data):
		if self.http_served:
			node_list = self.pipeline_info['node_order'][0]
			sent = {}
			for n in node_list:
				sent[n] = data

			sent_data = json.dumps(sent, cls=BytesEncoder, indent=2).encode()
			return sent_data











