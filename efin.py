import grequests
import aiohttp
from bs4 import BeautifulSoup
import aiohttp
import asyncio
from datetime import date
import asyncio
from pdb import set_trace

class eFinScraper:
    def __init__(self) -> None:
        self.joblist = []

    def search(self, titles, freq="d"):
        #### EFIN
        tfmap = {
            'd': 'ONE',
            'w': 'SEVEN',
            'm': 'SEVEN',
        }
        timefilter = tfmap[freq]

        # HongKong only now
        queries = [
            {
            'q': title,
            'radius': '50',
            'radiusUnit': 'km',
            'page': '1',
            'pageSize': '1000',
            'filters.positionType': 'PERMANENT',
            'filters.employmentType': 'FULL_TIME',
            #'filters.sectors': 'QUANTITATIVE_ANALYTICS|HEDGE_FUNDS|ASSET_MANAGEMENT|TRADING|DERIVATIVES|EQUITIES|RESEARCH|FINTECH|PRIVATE_EQUITY_VENTURE_CAPITAL',
            'filters.locationPath': 'Asia/Hong Kong',
            'filters.postedDate': timefilter,
            'language': 'en',
            } for title in titles
        ]
        self.joblist = search_ejoblist(queries)
        #### EFIN END

        print(f"Finished {len(self.joblist)} jobs from eFin")
        return self.joblist

efinancial_param = {
    'q': 'quant',
    'radius': '50',
    'radiusUnit': 'km',
    'page': '1',
    'pageSize': '100',
    'filters.positionType': 'PERMANENT',
    'filters.employmentType': 'FULL_TIME',
    #'filters.sectors': 'QUANTITATIVE_ANALYTICS|HEDGE_FUNDS|ASSET_MANAGEMENT|TRADING|DERIVATIVES|EQUITIES|RESEARCH|FINTECH|PRIVATE_EQUITY_VENTURE_CAPITAL',
    'filters.locationPath': 'Asia/Hong+Kong',
    'filters.postedDate': 'SEVEN',
    'language': 'en',
}

async def search_efinancial(params):
    print(f"Start e-search {params['q']}...")
    base_url="https://job-search-api.efinancialcareers.com/v1/efc/jobs/search"
    async with aiohttp.ClientSession() as s:
    
        async with s.get(base_url, params=params) as res:
            jobs = []
            json_res = await res.json()
            #set_trace()

            for job in json_res.get('data', []):
                record = {
                    'date': job['postedDate'],
                    'company': job['companyName'] if 'companyName' in job else '',
                    'title': job['title'],
                    'ap': 'https://www.efinancialcareers.hk' + job['detailsPageUrl'],
                    'link': 'https://www.efinancialcareers.hk' + job['detailsPageUrl'],
                    'des': job['score'],
                    'place': job['jobLocation']['city'] if 'jobLocation' in job else ''
                }
                jobs.append(record)
                
    print(f"Finish e-search {params['q']}.")
    return jobs

async def search_list(param_set):
    cs = ['date','title','company','ap','link','des','place']

    L = await asyncio.gather(
        *[search_efinancial(params=params) for params in param_set]
    )
    return [[j[c] for c in cs] for js in L for j in js]
    
def search_ejoblist(param_set):
    #asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(search_list(param_set))

if __name__ == '__main__':
    __spec__ = None
    ef = eFinScraper()
    ef.search(['quant'])
