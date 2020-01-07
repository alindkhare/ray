import ray
from ray.experimental.serve.context import TaskContext
from ray.experimental.serve.exceptions import RayServeException
from ray.experimental.serve.constants import (DEFAULT_HTTP_ADDRESS,
                                              PREDICATE_DEFAULT_VALUE)
from ray.experimental.serve.request_params import RequestParams, RequestInfo


class RayServeHandle:
    """A handle to a service endpoint.
    Invoking this endpoint with .remote is equivalent to pinging
    an HTTP endpoint.
    Example:
       >>> handle = serve.get_handle("my_endpoint")
       >>> handle
       RayServeHandle(
            Endpoint="my_endpoint",
            URL="...",
            Traffic=...
       )
       >>> handle.remote(my_request_content)
       ObjectID(...)
       >>> ray.get(handle.remote(...))
       # result
       >>> ray.get(handle.remote(let_it_crash_request))
       # raises RayTaskError Exception
    """

    def _check_slo_ms(self, request_slo_ms):
        if request_slo_ms is not None:
            request_slo_ms = float(request_slo_ms)
            if request_slo_ms < 0:
                raise ValueError(
                    "Request SLO must be positive, it is {}".format(
                        request_slo_ms))
        return request_slo_ms

    def _fix_kwarg_name(self, name):
        if name == "slo_ms":
            return "request_slo_ms"
        return name

    def _check_default_value(self, default_value, len_args, kwargs_keys):
        if default_value != PREDICATE_DEFAULT_VALUE:
            if not isinstance(default_value, tuple):
                raise ValueError("The default value must be a tuple.")
            if len(default_value) != 2:
                raise ValueError(
                    "Specify default_value in format: ('args',arg_index)"
                    " or ('kwargs', kwargs_key)")
            val = default_value[0]
            if val not in ["args", "kwargs"]:
                raise ValueError(
                    "First value of default_value must be: 'args' or 'kwargs'."
                )
            if val == "args":
                if default_value[1] >= len_args:
                    raise ValueError("Specify the args index currently!")
            else:
                if default_value[1] not in kwargs_keys:
                    raise ValueError("Specify the kwargs key correctly!")

    def __init__(self, router_handle, endpoint_name):
        self.router_handle = router_handle
        self.endpoint_name = endpoint_name

    def remote(self, *args, **kwargs):
        if len(args) != 0:
            raise RayServeException(
                "handle.remote must be invoked with keyword arguments.")

        # get request params defaults before enqueuing the query
        default_kwargs = RequestParams.get_default_kwargs()
        request_param_kwargs = {}
        for k in default_kwargs.keys():
            fixed_kwarg_name = self._fix_kwarg_name(k)
            request_param_kwargs[fixed_kwarg_name] = kwargs.pop(
                k, default_kwargs[k])

        try:
            # check if request_slo_ms specified is correct or not
            slo_ms = request_param_kwargs["request_slo_ms"]
            slo_ms = self._check_slo_ms(slo_ms)
            request_param_kwargs["request_slo_ms"] = slo_ms
        except ValueError as e:
            raise RayServeException(str(e))

        # check and pop predicate_condition and default value
        # specified while enqueuing
        predicate_condition = kwargs.pop("predicate_condition", True)
        default_value = kwargs.pop("default_value", PREDICATE_DEFAULT_VALUE)
        try:
            self._check_default_value(default_value, len(args),
                                      list(kwargs.keys()))
        except ValueError as e:
            raise RayServeException(str(e))

            # create request parameters required for enqueuing the request
        request_params = RequestParams(self.endpoint_name, TaskContext.Python,
                                       **request_param_kwargs)
        req_info_object_id = self.router_handle.enqueue_request.remote(
            request_params, predicate_condition, default_value, *args,
            **kwargs)

        # check if it is necessary to wait for enqueue to be completed
        # NOTE: This will make remote call completely non-blocking for
        #       certain cases.
        if RequestInfo.wait_for_requestInfo(request_params):
            req_info = ray.get(req_info_object_id)
            return_value = tuple(req_info)
            if len(return_value) == 1:
                return return_value[0]
            return return_value

    def get_traffic_policy(self):
        # TODO(simon): This method is implemented via checking global state
        # because we are sure handle and global_state are in the same process.
        # However, once global_state is deprecated, this method need to be
        # updated accordingly.
        return ray.get(
            self.router_handle.get_traffic.remote(self.endpoint_name))

    # TODO(simon): a convenience function that dumps equivalent requests
    # code for a given call.
