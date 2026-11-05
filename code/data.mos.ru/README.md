# Data.mos.ru

Портал открытых данных Правительства Москвы. Отвечает только с российских IP-адресов.

## Код

`collect.py` работает через API фронтенда `https://data.mos.ru/api/v2` (без ключа apidata.mos.ru):

* `GET /odata/datasets/count`, `POST /odata/datasets/search?limit=100&offset=N` — список всех наборов, включая архивные;
* `GET /odataExports/datasets?exportFormat=json|csv` — реестр наборов;
* `GET /odata/datasets/{id}`, `/versions`, `/releases` — карточка, версии, релизы;
* `GET /odataExports/meta?datasetId={id}&exportFormat=json|csv` — паспорт (у части наборов его нет: HTTP 500);
* `POST /odataExports/status {"datasetId": id}` — готовые файлы текущего релиза (JSON, CSV, XML, XLSX);
* `POST /odataExports/status {"datasetId": id, "format": "XLSX", "isAllVersions": true}` — XLSX исторических релизов;
* `GET /odata/MEDIA/getFile?id={fileId}` — ZIP-архив с файлом.

Этапы запускаются по очереди и докачивают недостающее:

```
export RUDATA_PROXY=socks5h://127.0.0.1:1080
python collect.py catalog --output DIR
python collect.py meta --output DIR --interval 0.5
python collect.py data --output DIR --interval 0.5
python collect.py exports --output DIR   # запросить сборку неготовых файлов, затем снова data
python collect.py history --output DIR   # необязательно: XLSX всех релизов
python collect.py report --output DIR
```

Запросы идут строго последовательно с паузой `--interval`. На HTTP 403/429 сбор останавливается.

robots.txt портала закрывает для всех `/api` и `/opendata/download`.

## Данные

* 2022 — первая выгрузка (старый портал).
* 2026-11-03 — 1 248 наборов (766 опубликованных, 482 архивных), метаданные всех наборов,
  4 197 файлов текущих релизов (JSON, CSV, XML, XLSX в ZIP), 8,2 ГБ.
  125 файлов портал не сформировал даже после запроса `exports` (статус `new`, `error` или нет `fileId`).
  История релизов не скачивалась: по статусам портала это 24 362 XLSX, около 709 ГБ.

Список наборов с итогами выгрузки — [data/datasets_20261103.csv](data/datasets_20261103.csv),
реестр портала — [data/register_20261103.csv](data/register_20261103.csv).
