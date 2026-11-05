# Порталы по Методическим рекомендациям

`collect.py` собирает порталы, которые публикуют наборы по стандарту
opendata.gosmonitor.ru/standard/3.0: реестр `list.csv` или HTML-список,
у каждого набора `meta.json` со списками версий данных (`data`) и структур (`structure`).

```
export RUDATA_PROXY=socks5h://127.0.0.1:1080
export RUDATA_CA_BUNDLE=$PWD/ca_bundle.pem   # см. ../certs

# opendata.mkrf.ru
python collect.py listcsv https://opendata.mkrf.ru/opendata/list.csv --output DIR
# data.ulgov.ru
python collect.py listhtml https://data.ulgov.ru/ 'https://data.ulgov.ru/index/list/page/{page}' --output DIR

python collect.py meta --output DIR
python collect.py data --output DIR                  # текущая версия и её структура
python collect.py data --output DIR --all-versions   # все версии
python collect.py report --output DIR
```

Запросы строго последовательные с паузой `--interval` (1 с по умолчанию). На HTTP 403/429 сбор останавливается.

robots.txt: opendata.mkrf.ru закрывает весь сайт (`Disallow: /`), data.ulgov.ru закрывает файлы `*.csv`, `*.json`, `*.xml`, `*.xlsx` и `/api/`.

## Данные

* opendata.mkrf.ru, 2026-11-03 — 63 набора в `list.csv`, у 55 есть `meta.json` (у 8 — HTTP 404/401).
  Скачаны текущие версии: 52 файла данных, 12,3 ГБ (`museum-exhibits` — ZIP 11,1 ГБ, 104 ГБ в распакованном виде)
  и 55 структур. 3 файла данных портал не отдаёт (404: `events`, `libraries`, `stat_concert_svod`).
  ZIP `egrkn/data-70-structure-16.json.zip` обрезан на стороне портала: размер совпадает с `Content-Length`,
  центрального каталога нет; `bsdtar -xOf` извлекает из него 719 МБ JSON.
  В `meta.json` перечислено 8 441 версия, прошлые версии не скачивались (`--all-versions`).
