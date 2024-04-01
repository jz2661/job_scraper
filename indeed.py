import os
import csv
import requests
from bs4 import BeautifulSoup
from datetime import date
import asyncio
from pdb import set_trace
from zenrows import ZenRowsClient

class IndeedScraper:
    def __init__(self) -> None:
        self.joblist = []

        self.client = ZenRowsClient(os.environ['ZENROWS_API_KEY'])

    def search_one_page(self, title, page):
        url = 'https://hk.indeed.com/jobs?q=' + title + \
                '&start=' + str(page * 10) + \
                '&sort=date'
        #response = requests.get(url, headers=self.headers)  

        client = ZenRowsClient(os.environ['ZENROWS_API_KEY'])
        response = client.get(url)

        print(response.text)

        response = requests.get(url, headers=headers)
        
        html = response.text

        # Scrapping the Web
        soup = BeautifulSoup(html, 'lxml')
        base_url = 'https://hk.indeed.com/viewjob?jk='
        d = soup.find('div', attrs={'id': 'mosaic-provider-jobcards'})

        jobs = soup.find_all('a', class_='tapItem')

        res = []
        for job in jobs:
            job_id = job['id'].split('_')[-1]
            job_title = job.find('span', title=True).text.strip()
            company = job.find('span', class_='companyName').text.strip()
            location = job.find('div', class_='companyLocation').text.strip()
            posted = job.find('span', class_='date').text.strip()
            job_link = base_url + job_id
            #print([job_title, company, location, posted, job_link])

            # Writing to CSV File
            res.append(
                [job_title, company, location.title(), posted, job_link])
        return res

    def search(self, titles, freq="d"):

        headers = {
            "User-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36"}

        # Skills and Place of Work
        skill = input('Enter your Skill: ').strip()
        place = input('Enter the location: ').strip()
        no_of_pages = int(input('Enter the #pages to scrape: '))

        # Name of the CSV File
        file_name = skill.title() + '_' + place.title() + '_Jobs.csv'
        # Path of the CSV File
        file_path = main_dir + file_name

        # Writing to the CSV File
        with open(file_path, mode='w') as file:
            columns = ['JOB_NAME', 'COMPANY', 'LOCATION', 'POSTED', 'APPLY_LINK']

            # Requesting and getting the webpage using requests
            print(f'\nScraping in progress...\n')
            for page in range(no_of_pages):
                url = 'https://www.indeed.co.in/jobs?q=' + skill + \
                    '&l=' + place + '&start=' + str(page * 10) + \
                    '&sort=date'
                response = requests.get(url, headers=headers)
                html = response.text

                # Scrapping the Web
                soup = BeautifulSoup(html, 'lxml')
                base_url = 'https://hk.indeed.com/viewjob?jk='
                d = soup.find('div', attrs={'id': 'mosaic-provider-jobcards'})

                jobs = soup.find_all('a', class_='tapItem')

                for job in jobs:
                    job_id = job['id'].split('_')[-1]
                    job_title = job.find('span', title=True).text.strip()
                    company = job.find('span', class_='companyName').text.strip()
                    location = job.find('div', class_='companyLocation').text.strip()
                    posted = job.find('span', class_='date').text.strip()
                    job_link = base_url + job_id
                    #print([job_title, company, location, posted, job_link])

                    # Writing to CSV File
                    writer.writerow(
                        [job_title, company, location.title(), posted, job_link])

        print(f'Jobs data written to <{file_name}> successfully.')

        print(f"Finished {len(self.joblist)} jobs from Indeed")
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
    ind = IndeedScraper()
    res = ind.search_one_page('quant',0)
