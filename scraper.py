import logging,os,sys
import pandas as pd
from datetime import datetime
from util import *
from efin import search_ejoblist
from glassdoor import search_glassdoor_joblist
from selenium.webdriver.chrome.options import Options
import concurrent.futures

from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, RemoteFilters

# Change root logger level (default is WARN)
logging.basicConfig(level = logging.WARN)

os.environ['LI_AT_COOKIE'] = r'AQEDAQ7-cxkFtGe8AAABglkj3I0AAAGCfTBgjVYAJL4aurh9YlOjhSAyQiKUf3UE-YfetnoFOnZ0zkHU1pgOfPy46h-8dFWzbDdpafxgg3I24YmVqYsrZbnERmdt-5CNvlFAuIuTOBwvIGAJY6rc4p_h'
wdr = r'C:\repo\linkedin'
os.chdir(wdr)

joblist = []

# Fired once for each successfully processed job
def on_data(data: EventData):
    #print('[ON_DATA]', data.title, data.company, data.company_link, data.date, data.link, data.insights, len(data.description))
    joblist.append(data)

# Fired once for each page (25 jobs)
def on_metrics(metrics: EventMetrics):
  print('[ON_METRICS]', str(metrics))

def on_error(error):
    print('[ON_ERROR]', error)

def on_end():
    print('[ON_END]')

options = Options()
options.add_argument("start-maximized")
options.add_argument("enable-automation")
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-browser-side-navigation")
options.add_argument("--disable-gpu")

print("BEFORE MAIN")
if __name__ == '__main__':
    __spec__ = None
    # We need monkey_patching according to the page below
    # https://github.com/miguelgrinberg/Flask-SocketIO/issues/65

    from gevent import monkey
    monkey.patch_all()

    titles = ['Quantitative','Derivative','Python','Option Trader','Market Making','Vice President','Structuring', \
                'QIS','Financial Engineering']
    #titles = ['QIS','Option Trader'] # test

    scraper = LinkedinScraper(
        chrome_executable_path=os.path.join(wdr, 'chromedriver.exe'), # Custom Chrome executable path (e.g. /foo/bar/bin/chromedriver) 
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

    print("BEFORE POOL")
    #pool = mp.Pool(len(titles))
    
    with concurrent.futures.ProcessPoolExecutor(2) as executor:

        print("AFTER POOL")
        #### Glassdoor
        tfmap = {
            'd': 1,
            'w': 7,
            'm': 30,
        }
        timefilter = tfmap[sys.argv[1]]

        queries = [
            {
            'q': title,
            'days': timefilter,
            } for title in titles
        ]
        
        gjoblist = []
        #gjoblist = search_glassdoor_joblist(queries)
        if 1:
            pool = [executor.submit(search_glassdoor_joblist, [i]) for i in queries]
            for i in concurrent.futures.as_completed(pool):
                try:
                    print(f'glassdoor jobs: {len(i.result())}')
                    gjoblist += i.result()
                except:
                    continue # anti scraping

        #### Glassdoor END


    #### LinkedIn Start
    tfmap = {
        'd': TimeFilters.DAY,
        'w': TimeFilters.WEEK,
        'm': TimeFilters.MONTH,
    }
    timefilter = tfmap[sys.argv[1]]

    queries = [
        Query(
            query=title,
            options=QueryOptions(
                #locations=['Singapore'],
                locations=['Hong Kong','Singapore'],
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

    if 0:
        pool = [executor.submit(scraper.run, [i]) for i in queries]
        for i in concurrent.futures.as_completed(pool):
            print(f'linkedin jobs: {len(i.result())}')

    #### LinkedIn End
    print("AFTER LINKEDIN")

    print("AFTER POOL CLOSE")

    #### EFIN
    tfmap = {
        'd': 'ONE',
        'w': 'SEVEN',
        'm': 'SEVEN',
    }
    timefilter = tfmap[sys.argv[1]]

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
    ejoblist=search_ejoblist(queries)
    #### EFIN END

    df = pd.DataFrame([expand_data(d) for d in joblist]+ejoblist+gjoblist,columns=['date','title','company','ap','link','des','place'])

    df = black(df)
    df = rank(df)
    df = remove_seen(df)
    df.to_excel('new.xlsx')

    send_mail(files=['new.xlsx'])
