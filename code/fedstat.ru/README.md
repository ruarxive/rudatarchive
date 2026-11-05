# Краулер fedstat.ru

По адресу [https://fedstat.ru/opendata/](https://fedstat.ru/opendata/) доступна страница с пагинацией, однако все ссылки
на ресурсы находятся в HTML файле. Таким образом сбор данных происходит так:
* Качаем URL https://fedstat.ru/opendata/, парсим адреса всех страниц конкретных наборов данных
* Качаем отдельные страницы, парсим метаданные, собираем из них pandas DataFrame. Данные сравнительно легко парсятся, так как используются микроформаты
* Качаем непосретсвенно открытые данные

Закачка происходит через прокси - сайт весьма стабильно работает через Tor. Закачка производится в [jupyter ноутбуке](Crawler.ipynb). 

Список собранных данных в файле [metadata_20220410.csv](data/metadata_20220410.csv). Имя файла - в поле _filename_. В случае если данные были недоступны - в поле _filename_ будет значение __missing__.

## 2026

[collect.py](collect.py) — то же самое скриптом, без Tor: портал отвечает только с российских IP (из-за рубежа 403), поэтому прокси задаётся через `RUDATA_PROXY`.

```
export RUDATA_PROXY=socks5h://127.0.0.1:1080
python collect.py catalog --output DIR   # список со страницы /opendata/
python collect.py pages --output DIR     # страницы наборов (gzip) и RDFa-метаданные в meta.json
python collect.py data --output DIR      # XML данных и XSD структуры
python collect.py report --output DIR    # metadata.csv
```

2026-11-03: 7 065 наборов.
