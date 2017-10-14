import commoncrawlscraper as ccs
import pandas as pd
def bis():
    url = 'http://www.bis.org/*'
    r = ccs.api.search_all_crawls(url)
    a = [x for x in r if '.pdf' in x['url']]
    df = pd.DataFrame(a)
    df = df.sort_values('timestamp')
    d = df.groupby('url').last()
    d = d.reset_index()
    dr = d.to_dict(orient='records')
    pdfs = list()
    for x in dr:
        print(x['url'])
        res = ccs.api.get_crawl_from_json(x)
        pdfs.append(res)
    return pdfs
