# -*- coding: utf-8 -*-
"""Archive of open data portals that follow the Russian methodological
recommendations (opendata.gosmonitor.ru/standard/3.0):
list.csv -> <identifier>/meta.json -> data-*.<fmt> and structure-*.<fmt> files.

Used for opendata.mkrf.ru (list.csv) and data.ulgov.ru (HTML list pages).

Stages (run in order, each one resumes from files on disk):

    listcsv   - list of datasets from <portal>/opendata/list.csv style registry
    listhtml  - list of datasets scraped from paginated HTML pages
    meta      - meta.json (and meta.csv) of every dataset
    data      - current data file and its structure (--all-versions: every version)
    report    - counts and sizes

Environment:
    RUDATA_PROXY      proxy for a Russian IP, e.g. socks5h://127.0.0.1:1080
    RUDATA_CA_BUNDLE  CA bundle with Russian Trusted Root CA (see ../certs)
"""
import csv
import datetime
import io
import json
import logging
import os
import re
import shutil
import sys
import time
from urllib.parse import unquote, urljoin, urlparse

import requests
import typer

USER_AGENT = 'Mozilla/5.0 (compatible; ruarxive-bot/1.0; +https://ruarxive.org)'
RETRY_STATUSES = (502, 503, 504)

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO)

app = typer.Typer()


class NoSpace(Exception):
    pass


def min_free():
    return int(os.environ.get('RUDATA_MIN_FREE_MB') or 0) * (1 << 20)


def check_fits(resp, filename):
    """Skip a file that would not fit into free space above the limit (downloaded elsewhere then)"""
    length = int(resp.headers.get('content-length') or 0)
    if min_free() and length and length > shutil.disk_usage(os.path.dirname(os.path.abspath(filename))).free - min_free():
        resp.close()
        raise NoSpace('file of %.1f MB does not fit into free space' % (length / 1e6))


def check_space(filename):
    """Stop a download without Content-Length before the disk is full"""
    if min_free() and shutil.disk_usage(os.path.dirname(os.path.abspath(filename))).free < min_free() // 2:
        raise NoSpace('free space below %d MB while downloading' % (min_free() // 2 >> 20))


def wait_for_disk(path):
    """Pause while free space is below RUDATA_MIN_FREE_MB (files are moved away by another process)"""
    limit = int(os.environ.get('RUDATA_MIN_FREE_MB') or 0) * (1 << 20)
    warned = False
    while limit and shutil.disk_usage(os.path.dirname(os.path.abspath(path))).free < limit:
        if not warned:
            logging.info('Free space below %d MB, waiting', limit >> 20)
            warned = True
        time.sleep(60)


class Blocked(Exception):
    pass


class Unavailable(Blocked):
    """Several requests in a row got no answer: the site or the route is down"""
    pass


class Client:
    def __init__(self, interval=1.0, retries=4, timeout=180):
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

    def request(self, method, url, stream=False, **kwargs):
        for attempt in range(1, self.retries + 1):
            self._wait()
            self.count += 1
            try:
                resp = self.session.request(method, url, timeout=self.timeout, stream=stream, **kwargs)
            except requests.RequestException as e:
                logging.warning('%s %s: %s (attempt %d)', method, url, e, attempt)
                time.sleep(10 * attempt)
                continue
            if resp.status_code in (403, 429):
                raise Blocked('%s %s -> HTTP %d' % (method, url, resp.status_code))
            if resp.status_code in RETRY_STATUSES:
                logging.warning('%s %s -> HTTP %d (attempt %d)', method, url, resp.status_code, attempt)
                time.sleep(10 * attempt)
                continue
            self.failed_in_row = 0
            return resp
        self.failed_in_row += 1
        if self.failed_in_row >= 3:
            raise Unavailable('%d requests in a row without answer, last %s' % (self.failed_in_row, url))
        raise RuntimeError('%s %s: no answer after %d attempts' % (method, url, self.retries))

    def get(self, url, **kwargs):
        resp = self.request('GET', url, **kwargs)
        resp.raise_for_status()
        return resp

    def download(self, url, filename):
        wait_for_disk(filename)
        resp = self.get(url, stream=True)
        check_fits(resp, filename)
        tmp = filename + '.part'
        size = 0
        try:
            with open(tmp, 'wb') as f:
                for n, chunk in enumerate(resp.iter_content(chunk_size=1 << 16)):
                    f.write(chunk)
                    size += len(chunk)
                    if n % 64 == 0:
                        check_space(filename)
        except BaseException:
            resp.close()
            if os.path.exists(tmp):
                os.remove(tmp)
            raise
        os.replace(tmp, filename)
        return resp, size


def write_bytes(filename, data):
    tmp = filename + '.part'
    with open(tmp, 'wb') as f:
        f.write(data)
    os.replace(tmp, filename)


def read_json(filename):
    with open(filename, 'r', encoding='utf8') as f:
        return json.load(f)


def read_datasets(root):
    with open(os.path.join(root, 'datasets.jsonl'), 'r', encoding='utf8') as f:
        return [json.loads(line) for line in f]


def write_datasets(root, rows):
    with open(os.path.join(root, 'datasets.jsonl'), 'w', encoding='utf8') as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')


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


def safe_name(value):
    return re.sub(r'[^\w.-]+', '_', value)[:150]


@app.command()
def listcsv(list_url: str, output: str = 'archive', interval: float = 1.0):
    """List of datasets from a list.csv registry (identifier,title,link,format)"""
    client = Client(interval)
    os.makedirs(output, exist_ok=True)
    resp = client.get(list_url)
    write_bytes(os.path.join(output, 'list.csv'), resp.content)
    text = resp.content.decode('utf-8-sig', errors='replace')
    rows = []
    for row in csv.DictReader(io.StringIO(text)):
        row = {k.strip().lower(): (v or '').strip() for k, v in row.items() if k}
        if not row.get('identifier'):
            continue
        link = row.get('link') or row.get('property') or ''
        rows.append({'identifier': row['identifier'], 'title': row.get('title'),
                     'meta_url': link, 'format': row.get('format')})
    write_datasets(output, rows)
    logging.info('Saved %d datasets from %s', len(rows), list_url)


@app.command()
def listhtml(base_url: str, page_url: str, output: str = 'archive', interval: float = 1.0,
             id_regex: str = r'href="/(\d{10}-[\w-]+)"', max_pages: int = 1000):
    """List of datasets from paginated HTML pages; page_url has {page} placeholder"""
    client = Client(interval)
    os.makedirs(output, exist_ok=True)
    found = {}
    for page in range(1, max_pages + 1):
        resp = client.get(page_url.format(page=page))
        ids = [i for i in dict.fromkeys(re.findall(id_regex, resp.text))]
        new = [i for i in ids if i not in found]
        for i in new:
            found[i] = {'identifier': i, 'title': None,
                        'meta_url': urljoin(base_url, '/%s/meta.json' % i), 'format': None}
        logging.info('Page %d: %d ids, %d new, total %d', page, len(ids), len(new), len(found))
        if not new:
            break
    write_datasets(output, list(found.values()))
    logging.info('Saved %d datasets', len(found))


@app.command()
def meta(output: str = 'archive', interval: float = 1.0, force: bool = False):
    """Download meta.json and meta.csv of every dataset"""
    client = Client(interval)
    datasets = read_datasets(output)
    errors = 0
    for n, ds in enumerate(datasets, 1):
        path = os.path.join(output, 'datasets', safe_name(ds['identifier']))
        os.makedirs(path, exist_ok=True)
        meta_url = ds['meta_url']
        if not meta_url.endswith('.json'):
            meta_url = re.sub(r'meta\.\w+$', 'meta.json', meta_url)
        for url in (meta_url, re.sub(r'\.json$', '.csv', meta_url)):
            filename = os.path.join(path, os.path.basename(urlparse(url).path))
            if not force and os.path.exists(filename):
                continue
            try:
                resp = client.request('GET', url)
                if resp.status_code != 200:
                    raise ValueError('HTTP %d' % resp.status_code)
                if filename.endswith('.json'):
                    json.loads(resp.content.decode('utf-8-sig'))
                write_bytes(filename, resp.content)
            except Blocked:
                raise
            except Exception as e:
                errors += 1
                logging.warning('[%d/%d] %s: %s', n, len(datasets), url, e)
                append_jsonl(os.path.join(output, 'meta_errors.jsonl'), {'identifier': ds['identifier'], 'url': url, 'error': str(e)})
        if n % 50 == 0:
            logging.info('[%d/%d] meta', n, len(datasets))
    logging.info('Meta done, %d requests, %d errors', client.count, errors)


def meta_files(meta, all_versions):
    data = [i for i in (meta.get('data') or []) if i.get('source')]
    structure = [i for i in (meta.get('structure') or []) if i.get('source')]
    if all_versions:
        return [('data', i) for i in data] + [('structure', i) for i in structure]
    if not data:
        return []
    current = max(data, key=lambda i: str(i.get('created') or i.get('id') or ''))
    files = [('data', current)]
    files += [('structure', i) for i in structure if i.get('id') == current.get('structure')]
    return files


@app.command()
def data(output: str = 'archive', interval: float = 1.0, all_versions: bool = False):
    """Download data and structure files listed in meta.json"""
    client = Client(interval)
    manifest = os.path.join(output, 'files_all.jsonl' if all_versions else 'files.jsonl')
    fetched = read_fetched(manifest)
    done, failed, skipped, size_total = 0, 0, 0, 0
    for ds in read_datasets(output):
        path = os.path.join(output, 'datasets', safe_name(ds['identifier']))
        metafile = os.path.join(path, 'meta.json')
        if not os.path.exists(metafile):
            continue
        with open(metafile, 'r', encoding='utf-8-sig') as f:
            meta = json.load(f)
        for kind, item in meta_files(meta, all_versions):
            url = urljoin(ds['meta_url'], item['source'].replace('\\', '/'))
            filedir = os.path.join(path, kind)
            os.makedirs(filedir, exist_ok=True)
            filename = os.path.join(filedir, safe_name(os.path.basename(unquote(urlparse(url).path))) or safe_name(str(item.get('id'))))
            if os.path.exists(filename) or os.path.relpath(filename, output) in fetched:
                skipped += 1
                continue
            try:
                resp, size = client.download(url, filename)
            except Blocked:
                raise
            except Exception as e:
                failed += 1
                logging.warning('%s %s: %s', ds['identifier'], url, e)
                append_jsonl(manifest, {'identifier': ds['identifier'], 'kind': kind, 'url': url, 'error': str(e)})
                continue
            done += 1
            size_total += size
            append_jsonl(manifest, {'identifier': ds['identifier'], 'kind': kind, 'url': url, **item,
                                    'filename': os.path.relpath(filename, output), 'size': size,
                                    'contentType': resp.headers.get('content-type')})
    logging.info('Files: %d downloaded (%.1f MB), %d already present, %d failed', done, size_total / 1e6, skipped, failed)


@app.command()
def report(output: str = 'archive'):
    """Print summary and write datasets.csv"""
    rows = []
    for ds in read_datasets(output):
        path = os.path.join(output, 'datasets', safe_name(ds['identifier']))
        meta = {}
        if os.path.exists(os.path.join(path, 'meta.json')):
            with open(os.path.join(path, 'meta.json'), 'r', encoding='utf-8-sig') as f:
                meta = json.load(f)
        counts = {}
        for kind in ('data', 'structure'):
            kdir = os.path.join(path, kind)
            files = os.listdir(kdir) if os.path.isdir(kdir) else []
            counts[kind] = (len(files), sum(os.path.getsize(os.path.join(kdir, f)) for f in files))
        rows.append({'identifier': ds['identifier'], 'title': meta.get('title') or ds.get('title'),
                     'creator': meta.get('creator'), 'modified': meta.get('modified'),
                     'versions': len(meta.get('data') or []), 'data_files': counts['data'][0],
                     'data_bytes': counts['data'][1], 'structure_files': counts['structure'][0]})
    with open(os.path.join(output, 'datasets.csv'), 'w', encoding='utf8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    typer.echo('Datasets: %d, with meta: %d' % (len(rows), sum(1 for r in rows if r['title'] and r['versions'])))
    typer.echo('Versions listed: %d' % sum(r['versions'] for r in rows))
    typer.echo('Without data file: %d' % sum(1 for r in rows if not r['data_files']))
    typer.echo('Downloaded: %d data files, %.1f MB' % (sum(r['data_files'] for r in rows), sum(r['data_bytes'] for r in rows) / 1e6))
    with open(os.path.join(output, 'report_info.json'), 'w', encoding='utf8') as f:
        json.dump({'reported': datetime.datetime.now().isoformat(timespec='seconds'), 'datasets': len(rows)}, f)


if __name__ == '__main__':
    try:
        app()
    except Unavailable as e:
        logging.error('Stopped, no answer: %s', e)
        sys.exit(3)
    except Blocked as e:
        logging.error('Stopped: %s', e)
        sys.exit(2)
