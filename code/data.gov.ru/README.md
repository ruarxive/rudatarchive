# Data.gov.ru

## Code
Script to collect catalog data collect.py (old portal, until 2022)

collect_v2.py collects the new portal (2025+) via its API `https://data.gov.ru/portal-back/api/v1`:

* `GET /dataset/page?search=publicationStatus::<id>&size=500&number=N` - list of published datasets
  (without the status filter the API answers 401 to anonymous users);
* `GET /dataset?id=<uuid>` - card with all data and structure versions (`data`, `structure`);
* `GET /storage?id=<fileId>` - file copy kept by the portal;
* `GET /organization/page?size=10000&search=approved::true`, `GET /nsi/<name>` - organizations and dictionaries.

```
export RUDATA_PROXY=socks5h://127.0.0.1:1080
python collect_v2.py catalog --output DIR
python collect_v2.py cards --output DIR --interval 0.5
python collect_v2.py data --output DIR --interval 0.5   # --all-versions for every version
python collect_v2.py report --output DIR
```

## Data 

Metadata file data/data.csv.xz list of all data packages

Downloaded data https://cdn.ruarxive.org/public/dataportals/data.gov.ru/2022-02-02/datagovru_20220202.zip 15GB compressed

2026-11-03: new portal, 7 034 published datasets, 1 423 organizations. Cards of 7 026 datasets
(8 answered 401), current data file and its structure for each of them: 14 050 files, 1.8 GB.
Cards list 30 385 versions in total; other versions were not downloaded (`--all-versions`).
Lists: data/datasets_20261103.csv.xz, data/organizations_20261103.json.xz.
