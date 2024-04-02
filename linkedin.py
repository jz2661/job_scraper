from bs4 import BeautifulSoup
import aiohttp
import asyncio
from datetime import date
import asyncio
import os

class LinkedInScraper:
    def __init__(self) -> None:
        self.joblist = []

        self.client = ZenRowsClient(os.environ['ZENROWS_API_KEY'])

    def search_one_page(self, title, page):
        title = title.replace(' ', '+')

        url = 'https://hk.indeed.com/jobs?q=' + title + \
                '&start=' + str(page * 10) + \
                '&sort=date'

        response = self.client.get(url)

        html = response.text

        # Scrapping the Web
        soup = BeautifulSoup(html, 'lxml')
        base_url = 'https://hk.indeed.com/viewjob?jk='
        d = soup.find('div', attrs={'id': 'mosaic-provider-jobcards'})

        jobs = soup.find_all('div', class_='job_seen_beacon')

        # ['date','title','company','ap','link','des','place']
        res = []
        for job in jobs:
            x = job.find('a')
            job_id = x['data-jk']
            job_title = job.find('span', title=True).text.strip()
            company = job.find('span', {"data-testid": "company-name"}).text.strip()
            location = job.find('div', {"data-testid": "text-location"}).text.strip()
            posted = job.find('span', {"data-testid": "myJobsStateDate"}).text.strip()
            des = job.find('ul').text.strip()
            job_link = base_url + job_id
            #print([job_title, company, location, posted, job_link])

            post_days = re.findall(r'\d+', posted)  # extracting number of days from posted_str
            job_date = datetime.now().isoformat()  # if days are not mentioned - using today
            if post_days:
                # calculated date of job posting if days are mentioned
                job_date = (datetime.now() - timedelta(days=int(post_days[0]))).isoformat()

            res.append(
                [job_date, job_title, company, job_link, job_link, len(des), location.title()])

        return res

    def search(self, titles, freq="d"):
        or_title = ' or '.join(titles)
        #self.joblist = query_async(partial(self.search_one_page,page=0), titles)
        for pg in range(5):
            self.joblist += self.search_one_page(or_title, page=pg)
            print(f"Finished page {pg}: {len(self.joblist)} jobs from Indeed")

        print(f"Finished {len(self.joblist)} jobs from Indeed")
        return self.joblist

if __name__ == '__main__':
    __spec__ = None
    lk = LinkedInScraper()
    lk.search(['quant'])
