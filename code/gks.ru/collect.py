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
LIST_FILE = 'data/list.csv'
PACKAGES_DIR = 'data/packages'

OPENDATA_LIST_URL = 'https://rosstat.gov.ru/opendata/list.csv'
PAGE_URL_PREFIX = 'https://rosstat.gov.ru/opendata/'

REQUEST_HEADER = {'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Mobile Safari/537.36'}
DEFAULT_CHUNK_SIZE = 4096
NAMESPACES = {
"http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v1_0/common" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v1_0/compact" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v1_0/cross" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v1_0/generic" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v1_0/query" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v1_0/structure" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v1_0/utility" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v2_0/common" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v2_0/cross" : None,
"http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact" : None,
"http://www.w3.org/2001/XMLSchema-instance" : None,
"http://www.w3.org/XML/1998/namespace" : None
}

def get_file(url, filename, aria2=False, aria2path=None):
    logging.info('Retrieving %s from %s' % (filename, url))
    page = requests.get(url, headers=REQUEST_HEADER, stream=True, verify=False)
    if not aria2:
        f = open(filename, 'wb')
        total = 0
        chunk = 0
        for line in page.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
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

def extract_page_meta(id):
    user_agent = {'User-agent': 'Mozilla/5.0'}
    response = requests.get(PAGE_URL_PREFIX + id, headers=user_agent, verify=False)

    soup = BeautifulSoup(response.text, "html.parser")


    input_field = soup.find("input", {'id': 'dataset_id'})
    if input_field:
        return {'dataset_id': input_field['value']}
    return None


def read_list(filename):
    if not os.path.exists(filename):
        urldata = requests.get(OPENDATA_LIST_URL, verify=False)
        f = open(filename, 'w', encoding='utf8')
        f.write(urldata.text)
        f.close()
    f = open(filename, 'r', encoding='utf8')
    reader = csv.DictReader(f)
    alist = []
    for r in reader:
        alist.append(r)
    f.close()
    return alist


@app.command()
def collectdata(force: bool = False):
    datalist = read_list(LIST_FILE)
    typer.echo(f"Datalist loaded")
    typer.echo(f'Updating metadata')
    for item in datalist:
        pkg_path = os.path.join(PACKAGES_DIR, item['property'])
        if not os.path.exists(pkg_path):
            os.makedirs(pkg_path)
        meta_j = os.path.join(pkg_path, 'meta.csv')
        if not os.path.exists(meta_j) or force:
            f = open(meta_j, 'w', encoding='utf8')
            resp = requests.get(PAGE_URL_PREFIX + item['property'] + '/meta.csv', verify=False)
            text = resp.content.decode('windows-1251')  
            f.write(text)
            f.close()
            typer.echo('Stored meta.csv of %s' % (item['property']))

    for item in datalist:
        pkg_path = os.path.join(PACKAGES_DIR, item['property'])
        if not os.path.exists(pkg_path):
            os.makedirs(pkg_path)
        meta_j = os.path.join(pkg_path, 'meta.csv')
        if os.path.exists(meta_j):
            f = open(meta_j, 'r', encoding='utf8')
            reader = csv.DictReader(f)
            meta = {}            
            for r in reader:
                try:               
                    meta[r['property']] = r['value']
                except:
                    continue
            f.close()
            urls = []
            for key in meta.keys():
                for block in ['data', 'structure']:
                    if key.find(block) > -1:
                        print(meta[key])
                        urls.append(meta[key])
            for u in urls:
                filename = os.path.join(pkg_path, u.rsplit('/', 1)[-1])
                if not os.path.exists(filename):
                    get_file(u, filename, aria2=True, aria2path='aria2c')
                    typer.echo('Saved %s of %s' % (u.rsplit('/', 1)[-1], item['property']))
                else:
                    typer.echo('Already saved %s of %s. Skip' % (u.rsplit('/', 1)[-1], item['property']))





if __name__ == "__main__":
    app()
