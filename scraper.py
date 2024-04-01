import logging,os,sys
import pandas as pd
from datetime import datetime
from util import *
from efin import eFinScraper
from glassdoor import GlassDoorScraper
from linkedin import LinkedInScraper
from functools import reduce

# Change root logger level (default is WARN)
logging.basicConfig(level = logging.WARN)

#wdr = r'C:\repo\linkedin'
#os.chdir(wdr)

class JobScraper:
    def __init__(self) -> None:
        self.joblist = []
        self.workers = [GlassDoorScraper(),eFinScraper(),LinkedInScraper()]
        self.titles = ['Quantitative','Derivative','Python','Option Trader','Market Making','Vice President','Structuring', \
                'QIS','Financial Engineering','Executive Director','Machine Learning','Data Science']

    def run(self, freq='d'):
        all_list = reduce(lambda x, y: x + y, [x.search(self.titles, freq) for x in self.workers[:2]])

        self.df = pd.DataFrame(all_list,columns=['date','title','company','ap','link','des','place'])
        self.df = black(self.df)
        self.df = rank(self.df)
        self.df = remove_seen(self.df)
        self.df.to_excel('new.xlsx')

        send_mail(files=['new.xlsx'])

if __name__ == '__main__':
    __spec__ = None
    js = JobScraper()
    js.run()
