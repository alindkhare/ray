from ray.experimental import serve
import uuid
class AbstractModel:
	def __init__(self,feature,input_shape='?',output_shape='?',input_type='?',output_type='?',num_inputs='?'):
		self.feature = feature
		self.num_inputs = num_inputs
		self.input_shape = input_shape
		self.output_shape = output_shape
		self.input_type = input_type
		self.output_type = output_type
		self.service_name = uuid.uuid1().hex
		# serve.create_no_http_service(self.service_name)
		self.is_linked = False
		self.backends = []
		# self.link_model()
	
	def get_config(self):
		if self.is_linked:
			d = {
				'input_shape' : self.input_shape,
				 'output_shape' : self.output_shape,
				 'input_type' : self.input_type,
				 'output_type' : self.output_type,
				 'num_inputs' : self.num_inputs
				}
			return d
		return None

	'''
	Currently this function is hard-coded
	'''
	def find_backends(self):
		if self.feature == "echo":
			backend_name = uuid.uuid4().hex
			def echo1(context):
				result = []
				batch_size = len(context)
				for i in range(batch_size):
					message = context[i]
					message += 'FROM MODEL ({}/BS_{}/{}) -> '.format(self.feature,batch_size,backend_name)
					result.append(message)
				return result
			return backend_name,echo1,None,0
		elif self.feature == "complex-echo":
			backend_name = uuid.uuid4().hex
			def echoC(*context):
				batch_size = len(context[0])
				result = []
				for i in range(batch_size):
					start = "[ "
					for val in context:
						start =  start + val[i] + " , "
						# message += ' FROM MODEL1 -> '
					start += " ] --> "
					start += 'FROM MODEL ({}/BS_{}/{}) -> '.format(self.feature,batch_size,backend_name)
					result.append(start)
				return result
			return backend_name,echoC,None,0
		elif self.feature == "imagenet-transform":
			import torchvision.transforms as transforms
			from PIL import Image
			import io
			import base64
			backend_name = uuid.uuid4().hex
			class Transform:
				def __init__(self,transform):
					self.transform = transform
				def __call__(self,batch_data):
					batch_size = len(batch_data)
					result = []
					for i in range(batch_size):
						data = Image.open(io.BytesIO(base64.b64decode(batch_data[i])))
						if data.mode != "RGB":
							data = data.convert("RGB")
						data = self.transform(data)
						# data = data.unsqueeze(0)
						result.append(data)
					return result

			min_img_size = 224
			transform = transforms.Compose([transforms.Resize(min_img_size),
                                         transforms.ToTensor(),
                                         transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                                              std=[0.229, 0.224, 0.225])])
			return backend_name,Transform,[transform],0

		elif self.feature == "imagenet-resnet":
			from torch.autograd import Variable
			from torchvision.models.resnet import resnet50
			import torch
			backend_name = uuid.uuid4().hex
			class PredictModelPytorch:
				def __init__(self, model):
					self.model = model

				def __call__(self, batch_data):
					# if 'transform' in context:
					# data = context['transform']
					data = torch.stack(batch_data)
					data = Variable(data)
					data = data.cuda()
					outputs = self.model(data)
					_, predicted = outputs.max(1)
					return predicted.cpu().numpy().tolist()

			model = resnet50(pretrained=True)
			model = model.cuda()

			return backend_name,PredictModelPytorch,[model],1
		return None

	def get_backend(self):
		if self.is_linked:
			return self.backends
	def link_model(self,max_batch_size=1):
		backend_info = self.find_backends()
		if backend_info is not None:
			serve.create_no_http_service(self.service_name,max_batch_size=max_batch_size)
			backend_name,cls_or_func,args,num_gpu = backend_info
			if args is None:
				serve.create_backend(cls_or_func, backend_name,num_gpu=num_gpu)
			else:
				serve.create_backend(cls_or_func, backend_name,num_gpu,*args)
			serve.link_service(self.service_name, backend_name)
			self.is_linked = True
			self.backends.append(backend_name)
		else:
			raise Exception('Backend not found for the AbstractModel!')




