# -*- coding: utf-8 -*-
import json
import os
from urllib.parse import quote
import requests
import logging
import typer
import csv
import time
import urllib.parse
import urllib3
import chardet
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#logging.getLogger().addHandler(logging.StreamHandler())
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG)

def enableVerbose():
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.DEBUG)


LIST_FILE = '../data/data.csv'
HEADERS_FILE = '../data/headers.csv'
RAW_DIR = '../data/raw'
PACKAGES_DIR = '../data/packages'
EXPORT_DIR = '../export'

REQUEST_HEADER = {'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Mobile Safari/537.36'}
DEFAULT_CHUNK_SIZE = 4096


app = typer.Typer()

def detect_encoding(filename, limit=1000000):
    f = open(filename, 'rb')
    chunk = f.read(limit)
    f.close()
    detected = chardet.detect(chunk)
    return detected

def read_list(listfile):
    f = open(listfile, 'r', encoding="utf8")
    data = []
    reader = csv.DictReader(f, delimiter=';')
    for row in reader:
        data.append(row)
    f.close()
    return data

CATALOG_URL = 'https://data.gov.ru/opendata/export/csv'

PAGE_URL = "https://data.gov.ru/opendata/%s"

def get_file(url, filename):
    logging.info('Retrieving %s from %s' % (filename, url))
    page = requests.get(url, headers=REQUEST_HEADER, stream=True, verify=False)
    headers = page.headers
    logging.debug('Status code %s' % (str(page.status_code)))
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
    return headers


@app.command()
def getcatalog():
    typer.echo('Datalist requested')
    response = requests.get(CATALOG_URL)
    f = open(LIST_FILE, 'w', encoding='utf8')
    f.write(response.text)
    f.close()
    typer.echo('Datalist stored')

@app.command()
def prepare(force: bool = False):
    datalist = read_list(LIST_FILE)
    typer.echo(f"Datalist loaded")
    typer.echo(f'Collecting metadata')
    n = 0
    for item in datalist:
        n += 1
        item_id = item['Идентификатор набор'].strip()
        typer.echo(f'Processing %d %s' % (n, item_id))
        pkg_path = os.path.join(PACKAGES_DIR, item_id)
        if not os.path.exists(pkg_path):
             os.makedirs(pkg_path)

        if not os.path.exists(os.path.join(pkg_path, 'meta.html')) or force:
            get_file(PAGE_URL % item_id, os.path.join(pkg_path, 'meta.html'))


@app.command()
def collectmeta(force: bool = False):
    datalist = read_list(LIST_FILE)
    typer.echo(f"Datalist loaded")
    typer.echo(f'Collecting metadata')
    for item in datalist['result']:
        item_id = item
        typer.echo(f'Processing %s' % (item_id))
        pkg_path = os.path.join(PACKAGES_DIR, item_id)
        if not os.path.exists(pkg_path):
             os.makedirs(pkg_path)

        if not os.path.exists(os.path.join(pkg_path, 'apibackuper.cfg')):
            typer.echo("- no cfg file found. Skip")
            continue
        if os.path.exists(os.path.join(pkg_path, 'data.jsonl')):
            typer.echo("- data.jsonl already generated. Skip")
            continue
        cwd = os.getcwd()
        os.chdir(pkg_path)
        os.system('apibackuper run full')
        os.system('apibackuper export jsonl data.jsonl')
        os.chdir(cwd)

@app.command()
def collectfiles(force: bool = False):
    datalist = read_list(LIST_FILE)
    typer.echo(f"Datalist loaded")
    typer.echo(f'Collecting files')
    for item in datalist:
        item_id = item['Идентификатор набор'].strip()
        data_url = item['Ссылка на версию набора']
        struct_url = item['Ссылка на структуру набора']
        typer.echo(f'Processing %s' % (item_id))
        pkg_path = os.path.join(PACKAGES_DIR, item_id)
        try:
            os.makedirs(pkg_path, exist_ok=True)
            os.makedirs(os.path.join(pkg_path, 'files'), exist_ok=True)
        except OSError:
            continue
        files = []
        for url in [data_url, struct_url]:

            pr = urllib.parse.urlparse(url)
            filename = pr.path.rsplit('/', 1)[-1]
            filename = urllib.parse.unquote(filename)
            parts = filename.rsplit('.', 1)
            if len(parts) == 1:
                ext = None
            else:
                ext = parts[1]
            filename = parts[0] + '.' + ext if ext else item['Формат'].lower()
            error = False
            headers = None
            error_msg = ""
            if not os.path.exists(os.path.join(pkg_path, 'files', filename)) and len(url) > 0:
                print('- retrieving %s' % (filename))
                try:
                    headers = get_file(url, os.path.join(pkg_path, 'files', filename))
                    headers = dict(headers)
                except Exception as e:
                    if hasattr(e, 'message'):
                        print('-', e.message)
                    else:
                        print('-', e)
                    error = True
                    error_msg = e.message if hasattr(e, 'message') else str(e)
                time.sleep(5)
            frec = {'id' : url, 'url' : url, 'ext' : ext if ext else item['Формат'].lower(), 'format' : item['Формат'], 'size' : os.path.getsize(os.path.join(pkg_path, 'files', filename)) if os.path.exists(os.path.join(pkg_path, 'files', filename)) else 0, 'filename' : filename, 'headers' : headers}
            if error:
                frec['error_msg'] = error_msg
                frec['error'] = error
            logging.info(str(frec))
            files.append(frec)

        filesfile = open(os.path.join(pkg_path, 'files.jsonl'), 'w', encoding='utf8')
        for r in files:
            filesfile.write(json.dumps(r) + '\n')
        filesfile.close()


SKIP_LIST = ['Resource Not Found', 'DOCTYPE', 'doctype']

def is_skipped(line):
    for w in SKIP_LIST:
        if line.find(w) > -1:
            return True
    return False


@app.command()
def extractheaders(force: bool = False):
    import csv
    datalist = read_list(LIST_FILE)
    typer.echo(f"Datalist loaded")
    typer.echo(f'Extracting CSV headers')
    n = 0
    empty = 0
    outfile = open(HEADERS_FILE, 'w', encoding='utf8')
    writer = csv.writer(outfile, delimiter=',')
    writer.writerow(['package','filename', 'n', 'name', 'fieldtype'])
    for item in datalist:
        n += 1
        item_id = item["Идентификатор набор"]
        pkg_path = os.path.join(PACKAGES_DIR, item_id)
        if not os.path.exists(pkg_path):
            continue


        if os.path.exists(os.path.join(pkg_path, 'files.jsonl')) and os.path.getsize(
            os.path.join(pkg_path, 'files.jsonl')) > 0:
            files = []
            resfile = open(os.path.join(pkg_path, 'files.jsonl'), 'r', encoding='utf8')
            for l in resfile:
                files.append(json.loads(l))
            resfile.close()
            print('Processing %s' % (item_id))
            for filedata in files:
                if filedata['ext'] != 'csv':
                    continue
                if filedata['filename'].find('structure-') != 0:
                    continue
                filename = filedata['filename']
                fullfilename = os.path.join(pkg_path, 'files', filename)
                if not os.path.exists(fullfilename):
                    print('- %s not exists. Skip' % (fullfilename))
                    continue
                det_encoding = detect_encoding(fullfilename)
                if det_encoding:
                    encoding = det_encoding['encoding']
                else:
                    encoding = 'utf8'
                f = open(fullfilename, 'r', encoding=encoding)
                try:
                    fline = f.readline().strip()
                except:
                    print('- %s charset decode error. Skip' % (fullfilename))
                    continue
                if is_skipped(fline):
                    print('- %s is not CSV. Skip' % (fullfilename))
                    continue
                delim = ';' if fline.count(';') > fline.count(',') else ','
                headers = fline.split(delim)
                f.seek(0)
                reader = csv.reader(f, delimiter=delim)
                try:
                    headers = next(reader)
                except:
                    print('- empty file')
                    continue
                if len(headers) != 4:
                    print("- unknown headers. Skip: " + ','.join(headers))
                    continue
                n = 0
                nameshift = 1
                if headers[0] == '№':
                    nameshift = 1
                elif headers[0] == 'field name':
                    nameshift = 0
                for row in reader:
                    if len(row) != 4:
                        print('- skip header %d, incomplete fields' % (n))
                        continue
                    h = row[nameshift].strip().strip('"').replace('\x00', '').replace('\t', ' ').replace('\n', ' ')
                    fieldtype = row[3].strip().strip('"').replace('\x00', '').replace('\t', ' ')
                    n += 1
                    writer.writerow([item_id.replace('\x00', '').replace('\t', ' '), filename.replace('\x00', '').replace('\t', ' '), str(n), h, fieldtype])
                print('- processed %d fields' % (n))
                f.close()
    outfile.close()


if __name__ == "__main__":
    app()
