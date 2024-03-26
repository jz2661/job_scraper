import types
import aiohttp
from bs4 import BeautifulSoup
import aiohttp
import asyncio
from datetime import date
import asyncio
import os
from selenium.webdriver.chrome.options import Options
import concurrent.futures

from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters

#globals
global_list = []
# Fired once for each successfully processed job
def on_data(data: EventData):
    #print('[ON_DATA]', data.title, data.company, data.company_link, data.date, data.link, data.insights, len(data.description))
    self.joblist.append(data)

# Fired once for each page (25 jobs)
def on_metrics(metrics: EventMetrics):
    print('[ON_METRICS]', str(metrics))

def on_error(error):
    print('[ON_ERROR]', error)

def on_end():
    print('[ON_END]')


class LinkedInScraper:
    def __init__(self) -> None:
        os.environ['LI_AT_COOKIE'] = r'AQEDAQ7-cxkFtGe8AAABglkj3I0AAAGCfTBgjVYAJL4aurh9YlOjhSAyQiKUf3UE-YfetnoFOnZ0zkHU1pgOfPy46h-8dFWzbDdpafxgg3I24YmVqYsrZbnERmdt-5CNvlFAuIuTOBwvIGAJY6rc4p_h'

        self.options = Options()
        self.options.add_argument("start-maximized")
        self.options.add_argument("enable-automation")
        self.options.add_argument("--headless")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-browser-side-navigation")
        self.options.add_argument("--disable-gpu")        

        self.joblist = []

    @staticmethod
    def expand_data(data):
        return (data.date, data.title, data.company, data.apply_link, data.link, len(data.description), data.place)

    def search(self, titles, freq="d"):
        # We need monkey_patching according to the page below
        # https://github.com/miguelgrinberg/Flask-SocketIO/issues/65

        from gevent import monkey
        monkey.patch_all()

        scraper = LinkedinScraper(
            chrome_executable_path=os.path.join('chromedriver.exe'), # Custom Chrome executable path (e.g. /foo/bar/bin/chromedriver) 
            chrome_options=None,  # Custom Chrome options here
            headless=True,  # Overrides headless mode only if chrome_options is None
            max_workers=1,  # How many threads will be spawned to run queries concurrently (one Chrome driver for each thread)
            slow_mo=0.8,  # Slow down the scraper to avoid 'Too many requests 429' errors (in seconds)
            page_load_timeout=15  # Page load timeout (in seconds)    
        )

        # Add event listeners
        scraper.on(Events.DATA, on_data)
        scraper.on(Events.ERROR, on_error)
        scraper.on(Events.END, on_end)

        #### LinkedIn Start
        tfmap = {
            'd': TimeFilters.DAY,
            'w': TimeFilters.WEEK,
            'm': TimeFilters.MONTH,
        }
        timefilter = tfmap[freq]

        queries = [
            Query(
                query=title,
                options=QueryOptions(
                    locations=['Hong Kong'],
                    apply_link = True,  # Try to extract apply link (easy applies are skipped). Default to False.
                    limit=200,
                    filters=QueryFilters(              
                        #company_jobs_url='https://www.linkedin.com/jobs/search/?f_C=1441%2C17876832%2C791962%2C2374003%2C18950635%2C16140%2C10440912&geoId=92000000',  # Filter by companies.
                        relevance=RelevanceFilters.RELEVANT,
                        time=timefilter,
                        type=[TypeFilters.FULL_TIME],
                        experience=None,                
                    )
                )
            ) for title in titles
        ]
        
        scraper.run(queries)

        #### LinkedIn End
        print(f"Finished {len(self.joblist)} jobs from LinkedIn")

        return [self.expand_data(d) for d in self.joblist]

  