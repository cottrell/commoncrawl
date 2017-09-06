import os
import re
import lib_cached
import mylib.tools

_mydir = os.path.dirname(os.path.realpath(__file__))

n_max = 3
data_dir = os.path.join(_mydir, 'data')

def max_limit(gen):
    for i, x in enumerate(gen):
        if i >= n_max:
            break
        yield x

class CC():
    def ls(self):
        return lib_cached.run_command_get_output('aws s3 ls s3://commoncrawl/crawl-data/')
    def get_paths(self, name):
        dirname = os.path.join(data_dir, name)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        for k in ['warc', 'wet', 'wat']:
            filename = os.path.join(dirname, '{}.paths.gz'.format(k))
            if os.path.exists(filename):
                print('filename exists, skipping: {}'.format(filename))
            else:
                cmd = 'cd {}  && ( curl -O https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2017-04/{}.paths.gz ; cd - ; )'.format(dirname, k)
                print('running {}'.format(cmd))
                mylib.tools.run_command_get_output(cmd)
    def get_paths_all(self):
        out, err, status = self.ls()
        out = [x.strip().rstrip('/') for x in out if '-MAIN-' in x]
        out = [re.sub('^PRE ', '', x) for x in out]
        for x in max_limit(out):
            self.get_paths(x)



cc = CC()

