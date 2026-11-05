# -*- coding: utf-8 -*-
"""Archive of the new data.gov.ru portal (2025+) via its API /portal-back/api/v1.

collect.py in this folder is for the old portal (archived 2022-02-02).

Stages (run in order, each one resumes from files on disk):

    catalog  - dictionaries, organizations, list of published datasets
    cards    - full card of every dataset (passport, list of data and structure files)
    data     - current data file and its structure file from the portal storage
               (--all-versions downloads every version listed in the card)
    report   - counts and sizes

Set RUDATA_PROXY (e.g. socks5h://127.0.0.1:1080) to go through a Russian IP.
Requests are strictly sequential with a pause.
"""
import csv
import datetime
import json
import logging
import os
import re
import shutil
import sys
import time
from urllib.parse import unquote, urlparse

import requests
import typer

BASE_URL = 'https://data.gov.ru/portal-back/api/v1'
USER_AGENT = 'Mozilla/5.0 (compatible; ruarxive-bot/1.0; +https://ruarxive.org)'
NSI = ['category', 'subject', 'format', 'org_type', 'tag', 'status', 'risk', 'document-type']
PUBLISHED_STATUS = 'c0a800d8-8a74-1694-818a-741742460000'
PAGE_SIZE = 500
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
        self.last = 0.0
        self.count = 0
        self.failed_in_row = 0

    def _wait(self):
        delay = self.last + self.interval - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        self.last = time.monotonic()

    def request(self, method, path, stream=False, **kwargs):
        url = path if path.startswith('http') else BASE_URL + path
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

    def json(self, path, **kwargs):
        resp = self.request('GET', path, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def download(self, path, filename, **kwargs):
        wait_for_disk(filename)
        resp = self.request('GET', path, stream=True, **kwargs)
        resp.raise_for_status()
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


def write_json(filename, data):
    tmp = filename + '.part'
    with open(tmp, 'w', encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    os.replace(tmp, filename)


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


def dataset_dir(root, dataset_id):
    path = os.path.join(root, 'datasets', dataset_id)
    os.makedirs(path, exist_ok=True)
    return path


def file_name(item):
    """<fileId>__<basename of source url>, source names repeat across versions"""
    base = os.path.basename(unquote(urlparse(item.get('source') or '').path))
    base = re.sub(r'[^\w.-]+', '_', base)[:150]
    return item['fileId'] + ('__' + base if base else '')


@app.command()
def catalog(output: str = 'archive', interval: float = 1.0):
    """Collect dictionaries, organizations and the list of published datasets"""
    client = Client(interval)
    nsidir = os.path.join(output, 'nsi')
    os.makedirs(nsidir, exist_ok=True)
    for name in NSI:
        resp = client.request('GET', '/nsi/%s' % name)
        if resp.status_code == 200:
            write_json(os.path.join(nsidir, '%s.json' % name), resp.json())
        else:
            logging.warning('NSI %s: HTTP %d', name, resp.status_code)
    orgs = client.json('/organization/page', params={'size': 10000, 'number': 0, 'search': 'approved::true'})
    write_json(os.path.join(output, 'organizations.json'), orgs['content'])
    logging.info('Organizations: %d of %d', len(orgs['content']), orgs['totalElements'])

    rows, number, total = [], 0, None
    while True:
        page = client.json('/dataset/page', params={'search': 'publicationStatus::' + PUBLISHED_STATUS,
                                                    'size': PAGE_SIZE, 'number': number, 'sort': 'lastChangeDate:desc'})
        total = page['totalElements']
        rows.extend(page['content'])
        logging.info('Datasets page %d: %d of %d', number, len(rows), total)
        if page['last'] or not page['content']:
            break
        number += 1
    unique = {r['id']: r for r in rows}
    with open(os.path.join(output, 'datasets.jsonl'), 'w', encoding='utf8') as f:
        for r in sorted(unique.values(), key=lambda r: r['id']):
            f.write(json.dumps(r, ensure_ascii=False) + '\n')
    logging.info('Saved %d datasets (%d rows, total %d)', len(unique), len(rows), total)
    if len(unique) != total:
        logging.warning('List size %d differs from total %d', len(unique), total)
    write_json(os.path.join(output, 'catalog_info.json'), {
        'collected': datetime.datetime.now().isoformat(timespec='seconds'),
        'total': total, 'listed': len(unique), 'organizations': len(orgs['content']), 'requests': client.count})


@app.command()
def cards(output: str = 'archive', interval: float = 1.0, force: bool = False):
    """Collect full card of every dataset"""
    client = Client(interval)
    datasets = read_datasets(output)
    errors = 0
    for n, ds in enumerate(datasets, 1):
        filename = os.path.join(dataset_dir(output, ds['id']), 'card.json')
        if not force and os.path.exists(filename):
            continue
        if n % 100 == 0:
            logging.info('[%d/%d] %s', n, len(datasets), ds['name'][:80])
        try:
            write_json(filename, client.json('/dataset', params={'id': ds['id']}))
        except Blocked:
            raise
        except Exception as e:
            errors += 1
            logging.error('Dataset %s: %s', ds['id'], e)
            append_jsonl(os.path.join(output, 'cards_errors.jsonl'), {'id': ds['id'], 'error': str(e)})
    logging.info('Cards done, %d requests, %d errors', client.count, errors)


def card_files(card, all_versions):
    """Files to fetch: current data file and its structure, or every version"""
    data = card.get('data') or []
    structure = card.get('structure') or []
    if all_versions:
        return [('data', i) for i in data if i.get('fileId')] + [('structure', i) for i in structure if i.get('fileId')]
    current = [i for i in data if i.get('fileId') == card.get('lastDatasetFile')]
    if not current and card.get('lastDatasetFile'):
        current = [{'fileId': card['lastDatasetFile']}]
    files = [('data', i) for i in current]
    struct_ids = {i.get('structureFileId') for i in current if i.get('structureFileId')}
    files += [('structure', i) for i in structure if i.get('fileId') in struct_ids]
    known = {i['fileId'] for _, i in files}
    files += [('structure', {'fileId': s}) for s in struct_ids if s not in known]
    return files


@app.command()
def data(output: str = 'archive', interval: float = 1.0, all_versions: bool = False):
    """Download data and structure files from the portal storage"""
    client = Client(interval)
    manifest = os.path.join(output, 'files_all.jsonl' if all_versions else 'files.jsonl')
    fetched = read_fetched(manifest)
    done, failed, skipped, size_total = 0, 0, 0, 0
    datasets = read_datasets(output)
    for n, ds in enumerate(datasets, 1):
        path = dataset_dir(output, ds['id'])
        if not os.path.exists(os.path.join(path, 'card.json')):
            continue
        card = read_json(os.path.join(path, 'card.json'))
        for kind, item in card_files(card, all_versions):
            filedir = os.path.join(path, kind)
            os.makedirs(filedir, exist_ok=True)
            filename = os.path.join(filedir, file_name(item))
            if os.path.exists(filename) or os.path.relpath(filename, output) in fetched:
                skipped += 1
                continue
            try:
                resp, size = client.download('/storage', filename, params={'id': item['fileId']})
            except Blocked:
                raise
            except Exception as e:
                failed += 1
                logging.warning('File %s of %s: %s', item['fileId'], ds['id'], e)
                append_jsonl(manifest, {'id': ds['id'], 'kind': kind, **item, 'error': str(e)})
                continue
            done += 1
            size_total += size
            append_jsonl(manifest, {'id': ds['id'], 'kind': kind, **item, 'filename': os.path.relpath(filename, output),
                                    'size': size, 'contentType': resp.headers.get('content-type'),
                                    'contentDisposition': resp.headers.get('content-disposition')})
        if n % 200 == 0:
            logging.info('[%d/%d] downloaded %d files, %.1f MB, %d failed', n, len(datasets), done, size_total / 1e6, failed)
    logging.info('Files: %d downloaded (%.1f MB), %d already present, %d failed', done, size_total / 1e6, skipped, failed)


@app.command()
def report(output: str = 'archive'):
    """Print summary of collected data and write datasets.csv"""
    rows = []
    for ds in read_datasets(output):
        path = os.path.join(output, 'datasets', ds['id'])
        card = read_json(os.path.join(path, 'card.json')) if os.path.exists(os.path.join(path, 'card.json')) else {}
        sizes = {}
        for kind in ('data', 'structure'):
            kdir = os.path.join(path, kind)
            files = os.listdir(kdir) if os.path.isdir(kdir) else []
            sizes[kind] = (len(files), sum(os.path.getsize(os.path.join(kdir, f)) for f in files))
        org = ds.get('organization') or {}
        rows.append({
            'id': ds['id'], 'identifier': ds.get('datasetId'), 'name': ds.get('name'),
            'organization': org.get('name'), 'inn': org.get('inn'), 'region': (org.get('region') or {}).get('name'),
            'category': (ds.get('category') or {}).get('name'), 'format': (ds.get('format') or {}).get('name'),
            'last_change': ds.get('lastChangeDate'), 'versions': len(card.get('data') or []),
            'data_files': sizes['data'][0], 'data_bytes': sizes['data'][1],
            'structure_files': sizes['structure'][0], 'source': ((card.get('data') or [{}])[0]).get('source'),
        })
    with open(os.path.join(output, 'datasets.csv'), 'w', encoding='utf8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    typer.echo('Datasets: %d, with card: %d' % (len(rows), sum(1 for r in rows if r['versions'] or r['source'])))
    typer.echo('Versions listed in cards: %d' % sum(r['versions'] for r in rows))
    typer.echo('Without data file: %d' % sum(1 for r in rows if not r['data_files']))
    typer.echo('Downloaded: %d data files, %.1f MB; %d structure files' % (
        sum(r['data_files'] for r in rows), sum(r['data_bytes'] for r in rows) / 1e6, sum(r['structure_files'] for r in rows)))


if __name__ == '__main__':
    try:
        app()
    except Unavailable as e:
        logging.error('Stopped, no answer: %s', e)
        sys.exit(3)
    except Blocked as e:
        logging.error('Stopped: %s', e)
        sys.exit(2)
