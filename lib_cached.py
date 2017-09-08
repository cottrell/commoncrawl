# put things in here to avoid reset cache on imp.reload
import mylib.tools
from functools import lru_cache
import requests

run_command_get_output = lru_cache()(mylib.tools.run_command_get_output)
requests_get = lru_cache()(requests.get)