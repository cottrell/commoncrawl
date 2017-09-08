import lib
import pickle
import json
import os
import urllib
import re
import lib_cached
import mylib.tools

_mydir = os.path.dirname(os.path.realpath(__file__))

n_max = -1
data_dir = os.path.join(_mydir, 'data')

def max_limit(gen):
    if n_max < 1:
        for x in gen:
            yield x
    else:
        for i, x in enumerate(gen):
            if i >= n_max:
                break
            yield x

class CC():
    def list_crawls(self):
        out, err, status = self._ls()
        out = [x.strip().rstrip('/') for x in out if '-MAIN-' in x]
        out = [re.sub('^PRE ', '', x) for x in out]
        return out
    def _ls(self):
        return lib_cached.run_command_get_output('aws s3 ls s3://commoncrawl/crawl-data/')
    def get_paths(self, crawl):
        dirname = os.path.join(data_dir, crawl)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        res = dict()
        for k in ['warc', 'wet', 'wat']:
            filename = os.path.join(dirname, '{}.paths.gz'.format(k))
            if os.path.exists(filename):
                print('filename exists, skipping: {}'.format(filename))
            else:
                cmd = 'cd {}  && ( curl -O https://commoncrawl.s3.amazonaws.com/crawl-data/{}/{}.paths.gz ; cd - ; )'
                cmd = cmd.format(crawl, dirname, k)
                print('running {}'.format(cmd))
                mylib.tools.run_command_get_output(cmd)
            res
    def get_paths_all_crawls(self):
        out = self.list_crawls()
        for x in max_limit(out):
            self.get_paths(x)
    def search_all_crawls(self, text):
        """ search across all known crawls """
        out = self.list_crawls()
        res = list()
        for crawl in max_limit(out):
            res.extend(self.search(crawl, text))
        return res
    def search(self, crawl, text):
        r = self._search(crawl, text)
        r = r.content.strip().split(b'\n')
        r = [json.loads(x) for x in r]
        return r
    def _search(self, crawl, text):
        text = urllib.parse.quote_plus(text)
        filename = os.path.join(data_dir, crawl, text) + '.pickle'
        if os.path.exists(filename):
            print('reading {}'.format(filename))
            return pickle.load(open(filename, 'rb'))
        url = 'http://index.commoncrawl.org/{}-index?url={}&output=json'.format(crawl, text)
        res = lib_cached.requests_get(url)
        print('writing {}'.format(filename))
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        pickle.dump(res, open(filename, 'wb'))
        return res

cc = CC()