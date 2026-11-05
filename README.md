# rudatarchive
Russian open data portals and websites archival campaign repository

Public list of portals and websites for archival https://airtable.com/shr1rzsajTM5SSyoI

Repository structure:

* /code - code related to archival of each data portal/website. Separated by folder like "data.gov.ru", "data.ulgov.ru"

## Guides

### How to do web archive

Please read instruction in Russian https://ruarxive.org/kb/instruments/howto-collect/make-copy-website

## Current status


* data.gov.ru - DONE
* rosstat.gov.ru/opendata - DONE
* opendata.mkrf.ru - DONE
* data.mos.ru - DONE
* fedstat.ru - DONE
* data.fss.ru - Partially done, site irresponsive
* hubofdata.ru - In progress
* data.ulgov.ru - Planned

## 2026 re-run

Collectors for the November 2026 re-run (data.mos.ru first, it is expected to close):

* data.mos.ru - [code/data.mos.ru/collect.py](code/data.mos.ru/collect.py), frontend API, all 1 248 datasets incl. archive
* data.gov.ru (new portal) - [code/data.gov.ru/collect_v2.py](code/data.gov.ru/collect_v2.py)
* opendata.mkrf.ru, data.ulgov.ru - [code/opendata-standard/collect.py](code/opendata-standard/collect.py)
* fedstat.ru - [code/fedstat.ru/collect.py](code/fedstat.ru/collect.py)
* data.fss.ru - [code/data.fss.ru/collect.py](code/data.fss.ru/collect.py) (API answered 504 on 2026-11-03)

Not collectable on 2026-11-03: rosstat.gov.ru and stat.customs.ru (anti-DDoS redirect `/DDoS01/`),
hubofdata.ru (bot challenge), showdata.gks.ru (no answer from any route).

Most portals answer only from Russian IP addresses: set `RUDATA_PROXY` (e.g. `socks5h://127.0.0.1:1080`).
Some use the Russian Trusted Root CA: see [code/certs](code/certs). `pip install -r requirements.txt`.

## More info about archival campaign

* Ruarxive website - https://ruarxive.org
* Ruarxive telegram channel - https://t.me/ruarxive

## Contacts 
Infoculture - infoculture@infoculture.ru, https://infoculture.ru

Ivan Begtin - ivan@begtin.tech, https://t.me/ibegtin
