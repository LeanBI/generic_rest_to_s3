#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on Dec 17, 2015

@author: Sébastien Brennion LeanBi
'''
import urllib
from   os import environ
import logging
from object_storage import object_storage
import urllib
import time
import datetime
from string import Template

class transaction_data():
    def __init__(self,**kwargs):
        self.base_url=environ.get("URL_DATA_TRANSACTION",None)
        self.storage=object_storage()
        self.intverval=environ.get("REST_POLLING_INTERVAL",30)
        self.file_name=environ.get("FILE_BASE_NAME","myFile-$date.txt")
        self.file_date_format=environ.get("FILE_DATE_FORMAT","%Hh%M")
        self.dir_name=environ.get("DIR_BASE_NAME","myDir-$date")
        self.dir_date_format=environ.get("DIR_DATE_FORMAT","%Y-%m-%d")

    def run(self):
        try:
            while True:
                data=self.get_data()
                self.store(data)
                time.sleep(self.intverval)
        except Exception as e:
            logging.exception(e)


    def get_data(self):
        logging.info("requesting : %s" % self.base_url)
        response=urllib.urlopen(self.base_url)
        response_str=response.read()
        return response_str


    def get_file_and_directory(self):
        now=datetime.datetime.now()
        dir_date={"date":now.strftime(self.dir_date_format)}
        dir_name=Template(self.dir_name).safe_substitute(dir_date)

        file_date={"date":now.strftime(self.file_date_format)}
        file_name=Template(self.file_name).safe_substitute(file_date)

        return dir_name, file_name

    def store(self,myString,**kwargs):
        d,f =self.get_file_and_directory()
        fileName="%s/%s/%s" %(environ.get("S3_KEY"),d , f)
        logging.debug("storing in file %s" % fileName)
        self.storage.put(environ["S3_BUCKET"],fileName,myString)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s  %(levelname)7s %(lineno)s %(name)s  - %(message)s',
                        level=getattr(logging,environ.get("LOG_LEVEL","INFO"))
                        )
    transaction_data().run()

    
    
    
    
    
    
    
    