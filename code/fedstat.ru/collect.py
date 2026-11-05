# -*- coding: utf-8 -*-
"""Archive of fedstat.ru/opendata (ЕМИСС). Script version of Crawler.ipynb.

The portal answers only from Russian IP addresses (403 from abroad).
Dataset metadata is published as RDFa on HTML pages, data as SDMX XML
generated on request.

Stages (run in order, each one resumes from files on disk):

    catalog  - list of dataset pages from https://fedstat.ru/opendata/
    pages    - dataset page (gzipped HTML) and RDFa metadata as meta.json
    data     - data XML and structure XSD listed in the metadata
    report   - counts, sizes and metadata.csv

Set RUDATA_PROXY (e.g. socks5h://127.0.0.1:1080) to go through a Russian IP.
Requests are strictly sequential with a pause.
"""
import csv
import datetime
import gzip
import json
import logging
import os
import re
import sys
import time
from urllib.parse import urljoin

import requests
import typer
from bs4 import BeautifulSoup

BASE_URL = 'https://fedstat.ru'
LIST_URL = 'https://fedstat.ru/opendata/'
USER_AGENT = 'Mozilla/5.0 (compatible; ruarxive-bot/1.0; +https://ruarxive.org)'
RETRY_STATUSES = (502, 503, 504)
NO_DATA_MARK = 'Нет данных для выбранного сочетания признаков'

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO)

app = typer.Typer()


class Blocked(Exception):
    pass


class Unavailable(Blocked):
    """Several requests in a row got no answer: the site or the route is down"""
    pass


class Client:
    def __init__(self, interval=1.0, retries=4, timeout=300):
        self.interval = interval
        self.retries = retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers['User-Agent'] = USER_AGENT
        proxy = os.environ.get('RUDATA_PROXY')
        if proxy:
            self.session.proxies = {'http': proxy, 'https': proxy}
            # otherwise HTTP_PROXY and system proxies take precedence over session.proxies
            self.session.trust_env = False
        if os.environ.get('RUDATA_CA_BUNDLE'):
            self.session.verify = os.environ['RUDATA_CA_BUNDLE']
        self.last = 0.0
        self.count = 0
        self.failed_in_row = 0

    def _wait(self):
        delay = self.last + self.interval - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        self.last = time.monotonic()

    def get(self, url, **kwargs):
        for attempt in range(1, self.retries + 1):
            self._wait()
            self.count += 1
            try:
                resp = self.session.get(url, timeout=self.timeout, **kwargs)
            except requests.RequestException as e:
                logging.warning('GET %s: %s (attempt %d)', url, e, attempt)
                time.sleep(10 * attempt)
                continue
            if resp.status_code in (403, 429):
                raise Blocked('GET %s -> HTTP %d' % (url, resp.status_code))
            if resp.status_code in RETRY_STATUSES:
                logging.warning('GET %s -> HTTP %d (attempt %d)', url, resp.status_code, attempt)
                time.sleep(10 * attempt)
                continue
            self.failed_in_row = 0
            return resp
        self.failed_in_row += 1
        if self.failed_in_row >= 3:
            raise Unavailable('%d requests in a row without answer, last %s' % (self.failed_in_row, url))
        raise RuntimeError('GET %s: no answer after %d attempts' % (url, self.retries))


def write_bytes(filename, data):
    tmp = filename + '.part'
    with open(tmp, 'wb') as f:
        f.write(data)
    os.replace(tmp, filename)


def write_json(filename, data):
    write_bytes(filename, json.dumps(data, ensure_ascii=False, indent=1).encode('utf8'))


def read_json(filename):
    with open(filename, 'r', encoding='utf8') as f:
        return json.load(f)


def read_datasets(root):
    with open(os.path.join(root, 'datasets.jsonl'), 'r', encoding='utf8') as f:
        return [json.loads(line) for line in f]


def append_jsonl(filename, record):
    with open(filename, 'a', encoding='utf8') as f:
        f.write(json.dumps(record, ensure_ascii=False) + '\n')


def read_fetched(manifest):
    """Files already downloaded according to the manifest; they may be moved away from this host"""
    fetched = set()
    if os.path.exists(manifest):
        with open(manifest, 'r', encoding='utf8') as f:
            for line in f:
                record = json.loads(line)
                if record.get('filename') and not record.get('error'):
                    fetched.add(record['filename'])
    return fetched


def parse_rdfa(html):
    """{about: {property: [values]}} from RDFa attributes of the page"""
    doc = BeautifulSoup(html, 'html.parser')
    result = {}
    for node in doc.select('[property]'):
        about = node.get('about')
        if about is None:
            for parent in node.parents:
                if parent.has_attr('about'):
                    about = parent['about']
                    break
        value = node.get('content', node.get_text(' ', strip=True))
        values = result.setdefault(about or '', {}).setdefault(node['property'], [])
        if value not in values:
            values.append(value)
    return result


def sources(meta):
    """(kind, url) for #data-N and #structure-N blocks"""
    found = []
    for about, props in meta.items():
        kind = 'data' if about.startswith('#data-') else 'structure' if about.startswith('#structure-') else None
        if kind:
            for url in props.get('dc:source', []):
                found.append((kind, url.replace('http://', 'https://', 1)))
    return found


@app.command()
def catalog(output: str = 'archive', interval: float = 1.0):
    """Collect list of dataset pages"""
    client = Client(interval)
    os.makedirs(output, exist_ok=True)
    resp = client.get(LIST_URL)
    resp.raise_for_status()
    write_bytes(os.path.join(output, 'opendata.html.gz'), gzip.compress(resp.content))
    links = re.findall(r'href="(/opendata/\d{10}-[\w-]+)"', resp.text)
    ids = list(dict.fromkeys(link.rsplit('/', 1)[1] for link in links))
    with open(os.path.join(output, 'datasets.jsonl'), 'w', encoding='utf8') as f:
        for i in ids:
            f.write(json.dumps({'identifier': i, 'url': urljoin(BASE_URL, '/opendata/' + i)}) + '\n')
    write_json(os.path.join(output, 'catalog_info.json'), {
        'collected': datetime.datetime.now().isoformat(timespec='seconds'), 'datasets': len(ids)})
    logging.info('Saved %d datasets (%d links)', len(ids), len(links))


@app.command()
def pages(output: str = 'archive', interval: float = 1.0, force: bool = False):
    """Download dataset pages and parse RDFa metadata"""
    client = Client(interval)
    datasets = read_datasets(output)
    errors = 0
    for n, ds in enumerate(datasets, 1):
        path = os.path.join(output, 'datasets', ds['identifier'])
        os.makedirs(path, exist_ok=True)
        if not force and os.path.exists(os.path.join(path, 'meta.json')):
            continue
        try:
            resp = client.get(ds['url'])
            resp.raise_for_status()
            write_bytes(os.path.join(path, 'page.html.gz'), gzip.compress(resp.content))
            write_json(os.path.join(path, 'meta.json'), parse_rdfa(resp.text))
        except Blocked:
            raise
        except Exception as e:
            errors += 1
            logging.error('%s: %s', ds['identifier'], e)
            append_jsonl(os.path.join(output, 'pages_errors.jsonl'), {'identifier': ds['identifier'], 'error': str(e)})
        if n % 200 == 0:
            logging.info('[%d/%d] pages, %d errors', n, len(datasets), errors)
    logging.info('Pages done, %d requests, %d errors', client.count, errors)


@app.command()
def data(output: str = 'archive', interval: float = 1.0):
    """Download data XML and structure XSD of every dataset"""
    client = Client(interval)
    manifest = os.path.join(output, 'files.jsonl')
    fetched = read_fetched(manifest)
    datasets = read_datasets(output)
    done, failed, skipped, nodata, size_total = 0, 0, 0, 0, 0
    for n, ds in enumerate(datasets, 1):
        path = os.path.join(output, 'datasets', ds['identifier'])
        metafile = os.path.join(path, 'meta.json')
        if not os.path.exists(metafile):
            continue
        for kind, url in sources(read_json(metafile)):
            filename = os.path.join(path, url.rsplit('/', 1)[1])
            if os.path.exists(filename) or os.path.exists(filename + '.nodata') or os.path.relpath(filename, output) in fetched:
                skipped += 1
                continue
            try:
                resp = client.get(url)
                resp.raise_for_status()
            except Blocked:
                raise
            except Exception as e:
                failed += 1
                logging.warning('%s %s: %s', ds['identifier'], url, e)
                append_jsonl(manifest, {'identifier': ds['identifier'], 'kind': kind, 'url': url, 'error': str(e)})
                continue
            head = resp.content[:4096].decode('utf8', errors='replace')
            if NO_DATA_MARK in resp.text[:20000] or '<!DOCTYPE html>' in head:
                # the portal answers with an HTML page when there is no data
                nodata += 1
                write_bytes(filename + '.nodata', resp.content)
                append_jsonl(manifest, {'identifier': ds['identifier'], 'kind': kind, 'url': url, 'error': 'no data'})
                continue
            write_bytes(filename, resp.content)
            done += 1
            size_total += len(resp.content)
            append_jsonl(manifest, {'identifier': ds['identifier'], 'kind': kind, 'url': url,
                                    'filename': os.path.relpath(filename, output), 'size': len(resp.content)})
        if n % 200 == 0:
            logging.info('[%d/%d] %d files, %.1f MB, %d no data, %d failed', n, len(datasets), done, size_total / 1e6, nodata, failed)
    logging.info('Files: %d downloaded (%.1f MB), %d already present, %d no data, %d failed',
                 done, size_total / 1e6, skipped, nodata, failed)


@app.command()
def report(output: str = 'archive'):
    """Print summary and write metadata.csv"""
    rows = []
    for ds in read_datasets(output):
        path = os.path.join(output, 'datasets', ds['identifier'])
        meta = read_json(os.path.join(path, 'meta.json')) if os.path.exists(os.path.join(path, 'meta.json')) else {}
        main = meta.get('/opendata/' + ds['identifier'], {})
        first = lambda props, key: (props.get(key) or [''])[0]
        files = [f for f in os.listdir(path)] if os.path.isdir(path) else []
        datafiles = [f for f in files if f.startswith('data-') and not f.endswith('.nodata')]
        rows.append({
            'identifier': ds['identifier'], 'title': first(main, 'dc:title'),
            'creator': first(main, 'dc:creator'), 'subject': first(main, 'dc:subject'),
            'created': first(main, 'dc:created'), 'valid': first(main, 'dc:valid'),
            'publisher_mbox': first(meta.get('#publisher', {}), 'foaf:mbox'),
            'data_source': ';'.join(u for k, u in sources(meta) if k == 'data'),
            'filename': ';'.join(datafiles) or 'missing',
            'size': sum(os.path.getsize(os.path.join(path, f)) for f in datafiles),
        })
    with open(os.path.join(output, 'metadata.csv'), 'w', encoding='utf8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    typer.echo('Datasets: %d, with metadata: %d' % (len(rows), sum(1 for r in rows if r['title'])))
    typer.echo('With data file: %d, %.1f MB' % (sum(1 for r in rows if r['filename'] != 'missing'), sum(r['size'] for r in rows) / 1e6))


if __name__ == '__main__':
    try:
        app()
    except Unavailable as e:
        logging.error('Stopped, no answer: %s', e)
        sys.exit(3)
    except Blocked as e:
        logging.error('Stopped: %s', e)
        sys.exit(2)
