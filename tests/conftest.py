import os
import tempfile

# Run commands in place: never build the managed conversion environment during tests
os.environ["DAG_CONVERTER_MANAGED_ENV"] = "1"
# Never touch the real conversion cache
_cache_home = tempfile.mkdtemp(prefix="dag-converter-test-cache-")
os.environ["XDG_CACHE_HOME"] = _cache_home
os.environ["LOCALAPPDATA"] = _cache_home
