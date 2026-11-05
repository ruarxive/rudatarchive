# -*- coding: utf-8 -*-
"""Archive of data.mos.ru (Moscow open data portal) via its frontend API /api/v2.

Stages (run in order, each one resumes from files on disk):

    catalog  - list of all datasets (published + archive) and register exports
    meta     - per dataset: card, versions, releases, passport, export statuses
    data     - files of the current release in every ready format (zip)
    history  - XLSX files of historical releases (only format the portal keeps)
    report   - counts and sizes

The portal answers only from Russian IP addresses. Set RUDATA_PROXY,
e.g. socks5h://127.0.0.1:1080. Requests are strictly sequential with a pause.
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
import zipfile

import requests
import typer

BASE_URL = 'https://data.mos.ru/api/v2'
USER_AGENT = 'Mozilla/5.0 (compatible; ruarxive-bot/1.0; +https://ruarxive.org)'
FORMATS = ['JSON', 'CSV', 'XML', 'XLSX']
REGISTER_FORMATS = ['json', 'csv']
PASSPORT_FORMATS = ['json', 'csv']
META_FILES = ['card.json', 'versions.json', 'releases.json', 'status.json', 'status_history.json'] + \
    ['passport.%s' % f for f in PASSPORT_FORMATS]
PAGE_SIZE = 100
READY_STATUSES = ('finished', 'archive')
# HTTP 500 with a JSON body is a permanent application error here, not worth a retry
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
    def __init__(self, interval=1.0, retries=4, timeout=120):
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

    def json(self, method, path, **kwargs):
        resp = self.request(method, path, **kwargs)
        resp.raise_for_status()
        return resp

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


def parse_size(value):
    """'69 KB' -> 70656, '356 b' -> 356"""
    m = re.match(r'([\d.,]+)\s*([a-zA-Z]+)', value or '')
    if not m:
        return 0
    mult = {'b': 1, 'kb': 1 << 10, 'mb': 1 << 20, 'gb': 1 << 30}.get(m.group(2).lower(), 1)
    return int(float(m.group(1).replace(',', '.')) * mult)


def dataset_dir(root, dataset_id):
    path = os.path.join(root, 'datasets', str(dataset_id))
    os.makedirs(path, exist_ok=True)
    return path


def data_filename(item):
    return 'data-v%s-r%s.%s.zip' % (item['versionNum'], item['releaseNum'], item['format'].lower())


@app.command()
def catalog(output: str = 'archive', interval: float = 1.0):
    """Collect list of datasets and register exports"""
    client = Client(interval)
    os.makedirs(output, exist_ok=True)
    total = client.json('GET', '/odata/datasets/count').json()['total']
    logging.info('Datasets total: %d', total)
    rows = []
    for offset in range(0, total + PAGE_SIZE, PAGE_SIZE):
        page = client.json('POST', '/odata/datasets/search', params={'limit': PAGE_SIZE, 'offset': offset}, json={}).json()
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
    unique = {r['id']: r for r in rows}
    with open(os.path.join(output, 'datasets.jsonl'), 'w', encoding='utf8') as f:
        for r in sorted(unique.values(), key=lambda r: r['id']):
            f.write(json.dumps(r, ensure_ascii=False) + '\n')
    logging.info('Saved %d datasets (%d rows)', len(unique), len(rows))
    if len(unique) != total:
        logging.warning('List size %d differs from count %d', len(unique), total)

    regdir = os.path.join(output, 'register')
    os.makedirs(regdir, exist_ok=True)
    for fmt in REGISTER_FORMATS:
        for translate in (False, True):
            suffix = '_en' if translate else ''
            filename = os.path.join(regdir, 'register%s.%s' % (suffix, fmt))
            resp = client.request('GET', '/odataExports/datasets',
                                  params={'exportFormat': fmt, 'translate': str(translate).lower()})
            if resp.status_code != 200:
                logging.warning('Register %s: HTTP %d', os.path.basename(filename), resp.status_code)
                continue
            with open(filename, 'wb') as f:
                f.write(resp.content)
            logging.info('Register %s: %d bytes', os.path.basename(filename), len(resp.content))
    write_json(os.path.join(output, 'catalog_info.json'), {
        'collected': datetime.datetime.now().isoformat(timespec='seconds'),
        'total': total, 'listed': len(unique), 'requests': client.count})


@app.command()
def meta(output: str = 'archive', interval: float = 1.0, force: bool = False):
    """Collect card, versions, releases, passport and export statuses of every dataset"""
    client = Client(interval)
    datasets = read_datasets(output)
    errors = 0
    for n, ds in enumerate(datasets, 1):
        try:
            meta_dataset(client, output, ds, force, '[%d/%d]' % (n, len(datasets)))
        except Blocked:
            raise
        except Exception as e:
            errors += 1
            logging.error('Dataset %s: %s', ds['id'], e)
            append_jsonl(os.path.join(output, 'meta_errors.jsonl'), {'datasetId': ds['id'], 'error': str(e)})
    logging.info('Meta done, %d requests, %d datasets with errors', client.count, errors)


def meta_dataset(client, output, ds, force, progress):
    dsid = ds['id']
    path = dataset_dir(output, dsid)
    has = lambda name: not force and os.path.exists(os.path.join(path, name))
    if all(has(name) for name in META_FILES):
        return
    logging.info('%s %s %s', progress, dsid, ds['fullName'][:80])
    if not has('card.json'):
        card = client.json('GET', '/odata/datasets/%d' % dsid).json()
        write_json(os.path.join(path, 'card.json'), card)
    if not has('versions.json'):
        versions = client.json('GET', '/odata/datasets/%d/versions' % dsid).json()
        write_json(os.path.join(path, 'versions.json'), versions)
    if not has('releases.json'):
        releases, offset, total = [], 0, None
        while total is None or offset < total:
            resp = client.json('GET', '/odata/datasets/%d/releases' % dsid, params={'limit': PAGE_SIZE, 'offset': offset})
            total = int(resp.headers.get('x-total-count') or 0)
            page = resp.json()
            releases.extend(page)
            offset += PAGE_SIZE
            if not page:
                break
        write_json(os.path.join(path, 'releases.json'), releases)
    for fmt in PASSPORT_FORMATS:
        if has('passport.%s' % fmt):
            continue
        # some datasets have no passport file: HTTP 500 "Ошибка при поиске файла паспорта НД"
        resp = client.request('GET', '/odataExports/meta', params={'datasetId': dsid, 'exportFormat': fmt})
        if resp.status_code == 200:
            with open(os.path.join(path, 'passport.%s' % fmt), 'wb') as f:
                f.write(resp.content)
        else:
            logging.warning('Passport %s of %s: HTTP %d', fmt, dsid, resp.status_code)
    if not has('status.json'):
        status = client.json('POST', '/odataExports/status', json={'datasetId': dsid}).json()
        write_json(os.path.join(path, 'status.json'), status)
    if not has('status_history.json'):
        history = client.json('POST', '/odataExports/status', json={'datasetId': dsid, 'format': 'XLSX', 'isAllVersions': True}).json()
        write_json(os.path.join(path, 'status_history.json'), history)


def fetch_files(client, output, items, manifest_name):
    """Download export files listed in status records"""
    manifest = os.path.join(output, manifest_name)
    fetched = read_fetched(manifest)
    done, failed, skipped, size_total = 0, 0, 0, 0
    for dsid, item in items:
        path = os.path.join(dataset_dir(output, dsid), 'data')
        os.makedirs(path, exist_ok=True)
        filename = os.path.join(path, data_filename(item))
        if os.path.exists(filename) or os.path.relpath(filename, output) in fetched:
            skipped += 1
            continue
        if item.get('status') not in READY_STATUSES or not item.get('fileId'):
            failed += 1
            append_jsonl(manifest, {'datasetId': dsid, **item, 'error': 'not ready'})
            continue
        try:
            resp, size = client.download('/odata/MEDIA/getFile', filename, params={'id': item['fileId']})
            if not zipfile.is_zipfile(filename):
                raise ValueError('not a zip file')
        except Blocked:
            raise
        except Exception as e:
            logging.warning('File %s of %s: %s', item['fileId'], dsid, e)
            if os.path.exists(filename):
                os.rename(filename, filename + '.bad')
            failed += 1
            append_jsonl(manifest, {'datasetId': dsid, **item, 'error': str(e)})
            continue
        done += 1
        size_total += size
        append_jsonl(manifest, {'datasetId': dsid, **item, 'filename': os.path.relpath(filename, output),
                                'size': size, 'contentDisposition': resp.headers.get('content-disposition')})
        if done % 50 == 0:
            logging.info('Downloaded %d files, %.1f MB, %d failed', done, size_total / 1e6, failed)
    logging.info('Files: %d downloaded (%.1f MB), %d already present, %d failed', done, size_total / 1e6, skipped, failed)


@app.command()
def data(output: str = 'archive', interval: float = 1.0, formats: str = ','.join(FORMATS)):
    """Download files of the current release in given formats"""
    client = Client(interval)
    wanted = [f.strip().upper() for f in formats.split(',')]
    items = []
    for ds in read_datasets(output):
        status_file = os.path.join(output, 'datasets', str(ds['id']), 'status.json')
        if not os.path.exists(status_file):
            logging.warning('No status for %s, run meta first', ds['id'])
            continue
        for item in read_json(status_file):
            if item.get('format') in wanted:
                items.append((ds['id'], item))
    expected = sum(parse_size(i['fileSize']) for _, i in items)
    logging.info('%d files to fetch, about %.1f MB', len(items), expected / 1e6)
    fetch_files(client, output, items, 'files.jsonl')


@app.command()
def exports(output: str = 'archive', interval: float = 1.0, wait: int = 600):
    """Ask the portal to build files that are not ready, then refresh status.json of those datasets"""
    client = Client(interval)
    pending = {}
    for ds in read_datasets(output):
        status_file = os.path.join(output, 'datasets', str(ds['id']), 'status.json')
        if not os.path.exists(status_file):
            continue
        for item in read_json(status_file):
            if item.get('status') not in READY_STATUSES or not item.get('fileId'):
                pending.setdefault(ds['id'], set()).add((item['format'], item.get('versionNum'), item.get('releaseNum')))
    logging.info('%d datasets with files not ready', len(pending))
    for dsid, items in pending.items():
        for fmt, version, release in sorted(items, key=str):
            body = {'datasetId': dsid, 'format': [fmt]}
            if version:
                body['version'] = version
            if release:
                body['releaseNum'] = release
            resp = client.request('POST', '/odataExports/export', json=body)
            if resp.status_code != 200:
                logging.warning('Export %s %s: HTTP %d %s', dsid, fmt, resp.status_code, resp.text[:200])
    logging.info('Exports requested, waiting %d s', wait)
    time.sleep(wait)
    still = 0
    for dsid in pending:
        status = client.json('POST', '/odataExports/status', json={'datasetId': dsid}).json()
        write_json(os.path.join(output, 'datasets', str(dsid), 'status.json'), status)
        still += sum(1 for i in status if i.get('status') not in READY_STATUSES or not i.get('fileId'))
    logging.info('Statuses refreshed, %d files still not ready', still)


@app.command()
def history(output: str = 'archive', interval: float = 1.0):
    """Download XLSX files of historical releases"""
    client = Client(interval)
    items = []
    for ds in read_datasets(output):
        status_file = os.path.join(output, 'datasets', str(ds['id']), 'status_history.json')
        if not os.path.exists(status_file):
            continue
        items.extend((ds['id'], item) for item in read_json(status_file))
    expected = sum(parse_size(i['fileSize']) for _, i in items)
    logging.info('%d history files, about %.1f MB', len(items), expected / 1e6)
    fetch_files(client, output, items, 'files_history.jsonl')


@app.command()
def report(output: str = 'archive'):
    """Print summary of collected data and write datasets.csv"""
    datasets = read_datasets(output)
    rows = []
    for ds in datasets:
        path = os.path.join(output, 'datasets', str(ds['id']))
        status = read_json(os.path.join(path, 'status.json')) if os.path.exists(os.path.join(path, 'status.json')) else []
        history_status = read_json(os.path.join(path, 'status_history.json')) if os.path.exists(os.path.join(path, 'status_history.json')) else []
        datadir = os.path.join(path, 'data')
        files = os.listdir(datadir) if os.path.isdir(datadir) else []
        rows.append({
            'id': ds['id'], 'name': ds['fullName'], 'status': ds['publicationStatusTagId'],
            'oiv': ds['oivName'], 'category': ds['thematicCategoryName'], 'is_dict': ds['isDict'],
            'actual_release_date': ds['actualReleaseDate'],
            'formats_ready': ','.join(sorted(i['format'] for i in status if i.get('status') in READY_STATUSES)),
            'size_current': sum(parse_size(i['fileSize']) for i in status),
            'history_files': len(history_status),
            'size_history': sum(parse_size(i['fileSize']) for i in history_status),
            'files_downloaded': len([f for f in files if f.endswith('.zip')]),
            'bytes_downloaded': sum(os.path.getsize(os.path.join(datadir, f)) for f in files if f.endswith('.zip')),
        })
    with open(os.path.join(output, 'datasets.csv'), 'w', encoding='utf8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    typer.echo('Datasets: %d (archive %d)' % (len(rows), sum(r['status'] == 'archive' for r in rows)))
    typer.echo('With metadata: %d' % sum(os.path.exists(os.path.join(output, 'datasets', str(r['id']), 'card.json')) for r in rows))
    typer.echo('Without ready files: %d' % sum(not r['formats_ready'] for r in rows))
    typer.echo('Current release size (portal): %.1f MB' % (sum(r['size_current'] for r in rows) / 1e6))
    typer.echo('History XLSX: %d files, %.1f MB' % (sum(r['history_files'] for r in rows), sum(r['size_history'] for r in rows) / 1e6))
    typer.echo('Downloaded: %d files, %.1f MB' % (sum(r['files_downloaded'] for r in rows), sum(r['bytes_downloaded'] for r in rows) / 1e6))


if __name__ == '__main__':
    try:
        app()
    except Unavailable as e:
        logging.error('Stopped, no answer: %s', e)
        sys.exit(3)
    except Blocked as e:
        logging.error('Stopped: %s', e)
        sys.exit(2)
