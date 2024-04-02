import os
import csv
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import asyncio
from pdb import set_trace
from zenrows import ZenRowsClient
import re
from util import query_async
from functools import partial
import json
import requests
import aiohttp

class JobsDBScraper:
    name = 'hkjobsdb'

    def __init__(self) -> None:
        self.joblist = []

    async def search_one_page(self, title, page):
        print(f"Start jobsdb-search {title} at page {page}")

        db_param = {
            'daterange': '3',
            'page': f'{page}',
            'sortmode': 'ListedDate',
        }

        title = title.replace(' ','-')

        url = f'https://hk.jobsdb.com/{title}-jobs/'

        async with aiohttp.ClientSession() as client:
            async with client.get(url,params=db_param) as response:

                html = await response.text()

                # Scrapping the Web
                soup = BeautifulSoup(html, 'lxml')

                d = soup.find('div', attrs={'id': 'mosaic-provider-jobcards'})

                jobs = soup.find_all('article')
                base_url = 'https://hk.jobsdb.com/job/'
                # ['date','title','company','ap','link','des','place']
                res = []
                for job in jobs:
                    try:
                        x = job.find('a')
                        job_id = job.get('data-job-id')
                        job_title = job.find('a', {"data-automation": "jobTitle"}).text.strip()
                        company = job.find('a', {"data-automation": "jobCompany"}).text.strip()
                        location = job.find('a', {"data-automation": "jobLocation"}).text.strip()
                        posted = job.find('span', {"data-automation": "jobListingDate"}).text.strip()

                        des = job.find('ul').text.strip() if job.find('ul') else ''
                        job_link = base_url + job_id
                        #print([job_title, company, location, posted, job_link])

                        post_days = re.findall(r'\d+', posted)  # extracting number of days from posted_str
                        job_date = datetime.now().isoformat()  # if days are not mentioned - using today
                        if post_days:
                            # calculated date of job posting if days are mentioned
                            job_date = (datetime.now() - timedelta(days=int(post_days[0]))).isoformat()

                        res.append(
                            [job_date, job_title, company, job_link, job_link, len(des), location.title()])
                    except:
                        # weird format issues
                        continue

        print(f"Finish jobsdb-search {title} at page {page}: {len(res)} jobs")

        return res

    async def search_list(self, titles):
        param_set = [{'title': title, 'page': pg} for title in titles for pg in range(1,3)]

        L = await asyncio.gather(
            *[self.search_one_page(**params) for params in param_set]
        )
        return [j for js in L for j in js]

    def search(self, titles, freq="d"):
        #TODO: page>1
        self.joblist = asyncio.run(self.search_list(titles))

        print(f"Finished {len(self.joblist)} jobs from JobsDB")
        return self.joblist

if __name__ == '__main__':
    __spec__ = None
    ind = JobsDBScraper()
    #res = ind.search_one_page('quant',0)
    res = ind.search(['quant','head'])
