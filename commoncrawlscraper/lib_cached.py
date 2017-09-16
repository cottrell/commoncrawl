"""
Put things in here to avoid reset cache on imp.reload.
"""
import mylib.tools
from functools import lru_cache
import requests
import gzip
import os

run_command_get_output = lru_cache()(mylib.tools.run_command_get_output)
requests_get = lru_cache()(requests.get)

# cached and persisted
@lru_cache()
def run_cmd_with_cachefile(filename, cmd, *args, return_result=True, **kwargs):
    if not os.path.exists(filename):
        print('running {}'.format(cmd))
        run_command_get_output(cmd)
        assert os.path.exists(filename), 'filename {} was not created!'.format(filename)
    if return_result:
        if filename.endswith('.gz'):
            return gzip.open(filename).read()
        else:
            return open(filename).read()
