import lib
import pickle
import json
import os
import urllib
import re
import lib_cached
import mylib.tools

_mydir = os.path.dirname(os.path.realpath(__file__))

n_max = 3
data_dir = os.path.join(_mydir, 'crawl-data') # some things will break if this is not called crawl-data

_baseurl = "https://commoncrawl.s3.amazonaws.com"

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
    def get_paths(self, crawl, return_result=False):
        dirname = os.path.join(data_dir, crawl)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        res = dict()
        for k in ['warc', 'wet', 'wat']:
            filename = os.path.join(dirname, '{}.paths.gz'.format(k))
            cmd = 'curl {baseurl}/crawl-data/{crawl}/{filetype}.paths.gz > {dirname}/{filetype}.paths.gz'
            cmd = cmd.format(crawl=crawl, baseurl=_baseurl, dirname=dirname, filetype=k)
            res[k] = lib_cached.run_cmd_with_cachefile(filename, cmd, return_result=return_result)
        if return_result:
            res = {k: v.decode().split('\n') for k, v in res.items()}
            return res
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
        text = urllib.parse.quote_plus(text)
        filename = os.path.join(data_dir, crawl, text) + '.json'
        url = 'http://index.commoncrawl.org/{}-index?url={}&output=json'.format(crawl, text)
        cmd = "curl -s '{}' > {}".format(url, filename)
        r = lib_cached.run_cmd_with_cachefile(filename, cmd, return_result=True)
        r = r.strip().split('\n')
        r = [json.loads(x) for x in r]
        return r
    def get_crawl_from_json(self, json_entry, return_result=True):
        """
        curl -H "range: bytes=822555329-822557378" -O https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2017-04/segments/1484560279657.18/warc/CC-MAIN-20170116095119-00156-ip-10-171-10-70.ec2.internal.warc.gz
        """
        start = int(json_entry['offset'])
        stop = start + int(json_entry['length'])
        filename = json_entry['filename']
        cmd = "curl -H 'range: {start}-{stop}' -O '{baseurl}/{filename}' > {filename}"
        cmd = cmd.format(start=start, stop=stop, baseurl=_baseurl, filename=filename)
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return lib_cached.run_cmd_with_cachefile(filename, cmd, return_result=return_result)

cc = CC()