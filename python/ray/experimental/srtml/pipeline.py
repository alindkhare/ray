import uuid
from ray.experimental import serve
from collections import defaultdict, deque

class Pipeline:
	def __init__(self):
		self.abstract_models_obj = {}
		self.abstract_models_config = defaultdict(dict)
		self.pipeline_name = uuid.uuid1().hex
		self.inverted_service_dependency = defaultdict(list)
		self.provisioned = False
		self.pipeline_info = None
		self.pipeline_handle = None
	def fill_config(self,model):
		if not self.provisioned:
			config = model.get_config()
			if config is not None:
				self.abstract_models_config[model.service_name] = config
			else:
				raise Exception('AbstractModel has no backend!')

	def add_model(self,model):
		if not self.provisioned:
			if model.service_name not in abstract_models_obj:
				self.abstract_models_obj[model.service_name] = model
				self.fill_config(model)

	def __isWildCard(self,expr):
		return expr == '?'
	def __addSanityCheck(self,model1_sname,model2_sname):
		if not self.provisioned:
			final_check = True
			m1_config = self.abstract_models_config[model1_sname]
			m2_config = self.abstract_models_config[model2_sname]

			if __isWildCard(self,m2_config['num_inputs']) or m2_config['num_inputs'] > 0:
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

	def provision_pipeline(self,http=False):
		if not self.provisioned:
			for service1 in self.inverted_service_dependency.keys():
				directed_edges = self.inverted_service_dependency[service1]
				for service2 in directed_edges:
					serve.add_service_dependencies(self.pipeline_name,service2,service1)
			serve.provision_pipeline(self.pipeline_name)
			self.pipeline_info = serve.get_service_dependencies(self.pipeline_name)
			self.pipeline_handle = serve.get_handle(self.pipeline_name)
			self.provisioned = True

	def remote(self,data):
		if self.provisioned:
			node_list = self.pipeline_info['node_order'][0]
			sent = {}
			for n in node_list:
				sent[n] = data
			return self.pipeline_handle.remote(**sent)









