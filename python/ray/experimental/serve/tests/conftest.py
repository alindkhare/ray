import os
import tempfile

import pytest

import ray
from ray.experimental import serve


@pytest.fixture(scope="session")
def serve_instance():
    ray_already_initialized = ray.is_initialized()
    _, new_db_path = tempfile.mkstemp(suffix=".test.db")
    serve.init(
        kv_store_path=new_db_path,
        blocking=True,
        ray_init_kwargs={"num_cpus": 36})
    yield
    os.remove(new_db_path)
    if not ray_already_initialized:
        ray.shutdown()


@pytest.fixture(scope="session")
def ray_instance():
    ray_already_initialized = ray.is_initialized()
    if not ray_already_initialized:
        ray.init(object_store_memory=int(1e8))
    yield
    if not ray_already_initialized:
        ray.shutdown()
