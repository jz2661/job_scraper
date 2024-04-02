import logging,os,sys
import pandas as pd
from datetime import datetime
from util import *
from efin import eFinScraper
from glassdoor import GlassDoorScraper
from indeed import IndeedScraper
from jobsdb import JobsDBScraper
from linkedin import LinkedInScraper
from functools import reduce

# Change root logger level (default is WARN)
logging.basicConfig(level = logging.WARN)

#indeed 5 pages (3days), jobsdb 2 pages each (1wk), efin 1wk

class JobScraper:
    def __init__(self) -> None:
        self.joblist = []
        self.workers = [JobsDBScraper(),IndeedScraper(),eFinScraper(),]
        self.titles = ['Quantitative','Head','Lead','Vice President','Algorithm', \
                'Director','Machine Learning','Data Science','Scientist','Stratagy','AI']

    def run(self, freq='d'):
        all_list = reduce(lambda x, y: x + y, [x.search(self.titles, freq) for x in self.workers[:]])

        self.df = pd.DataFrame(all_list,columns=['date','title','company','ap','link','des','place'])
        self.df = black(self.df)
        self.df = rank(self.df)
        try:
            self.df = remove_seen(self.df)
        except:
            logging.WARN("Remove seen failed")
        self.df.to_excel('new.xlsx')

        send_mail(files=['new.xlsx'])

if __name__ == '__main__':
    __spec__ = None
    js = JobScraper()
    js.run(freq='d')
