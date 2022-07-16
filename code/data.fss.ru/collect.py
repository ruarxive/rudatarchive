# -*- coding: utf-8 -*-
import json
import os
import re
import sys
from csv import DictReader
from urllib.request import urlretrieve
from urllib.parse import quote
from zipfile import ZipFile
import gzip
import requests
import logging
import typer
from bs4 import BeautifulSoup
import datetime
import csv
import chardet
import bson
import xmltodict
import zipfile
import shutil
PACKAGES_DIR = 'data/packages'

DATA_MAP = {'https://data.fss.ru/open/api/openData' : 'opendata.json',
            'https://data.fss.ru/open/api/sections' : 'sections.json',
            'https://data.fss.ru/open/api/getRegIndicators' : 'indicators.json',
            'https://data.fss.ru/open/api/getAllInfoSections' : 'allinfo.json',
            'https://data.fss.ru/open/api/allDiagrams' : 'alldiagrams.json'}


META_DATA_PATTERN = 'https://data.fss.ru/open/api/openDataPass?idIndicator=%s'
CSV_DATA_PATTERN = 'https://data.fss.ru/open/api/file?idIndicator=%s'

def get_file(url, filename, aria2=False, aria2path=None):
    logging.info('Retrieving %s from %s' % (filename, url))
    page = requests.get(url, stream=True, verify=False)
    if not aria2:
        f = open(filename, 'wb')
        total = 0
        chunk = 0
        for line in page.iter_content():
            chunk += 1
            if line:
                f.write(line)
            total += len(line)
            if chunk % 1000 == 0:
                logging.debug('File %s to size %d' % (filename, total))
        f.close()
    else:
        dirpath = os.path.dirname(filename)
        basename = os.path.basename(filename)
        if len(dirpath) > 0:
            s = "%s --retry-wait=10 -d %s --out=%s %s" % (aria2path, dirpath, basename, url)
        else:
            s = "%s --retry-wait=10 --out=%s %s" % (aria2path, basename, url)
        logging.info('Aria2 command line: %s' % (s))
        os.system(s)



app = typer.Typer()


def read_list(filename):
    f = open(filename, 'r', encoding='utf8')
    alist = json.load(f)
    f.close()
    return alist


@app.command()
def collectdata(force: bool = False):
    for url, filename in DATA_MAP.items():
        if not os.path.exists(os.path.join('data', filename)):
            get_file(url, os.path.join('data', filename))
            print('Saved %s to %s' % (url, os.path.join('data', filename)))
    data = read_list('data/opendata.json')
    for item in data:
        id = item['idIndicator']
        filename = os.path.join('data', 'packages', '%s_meta.json' % (str(id)))
        if not os.path.exists(filename):
            get_file(META_DATA_PATTERN % str(id), filename)
        filename = os.path.join('data', 'packages', '%s_data.csv' % (str(id)))
        if not os.path.exists(filename):
            get_file(CSV_DATA_PATTERN % str(id), filename)
        print('Stored %s' % (str(id)))





if __name__ == "__main__":
    app()
