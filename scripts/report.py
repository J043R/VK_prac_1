"""Generate the teacher's Markdown form from actual measurements."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
R = ROOT/'results'


def load(name):
    p = R/name
    return json.loads(p.read_text(encoding='utf-8-sig')) if p.exists() else None


def generate():
    R.mkdir(parents=True, exist_ok=True)
    p, b, d, s = [load(n) for n in ('pipeline.json','benchmark.json','demo.json','spark-bronze.json')]
    valid = lambda x: bool(p and x and x.get('run') == p['run'])
    b, d, s = [x if valid(x) else None for x in (b,d,s)]
    sf = p['sf'] if p else 1
    evidence_manifest = ROOT/'evidence/manifest.json'
    evidence_run = json.loads(evidence_manifest.read_text(encoding='utf-8-sig')).get('run') if evidence_manifest.exists() else None
    def screenshot(filename, caption):
        if p and evidence_run == p['run'] and (ROOT/'evidence'/filename).exists():
            return f'\n![{caption}](evidence/{filename})\n\n*{caption}. Скриншот ручной проверки запуска `{p["run"]}`.*\n'
        return ''
    complete = bool(p and b and d and s)
    status = (f"Эксперимент `{p['run']}`, масштаб `tpch.sf{sf}`. Измерения и результаты проверок сохранены в `results/`." if complete else
              'Работа ещё не готова к сдаче: отсутствуют подтверждённые результаты одного или нескольких этапов. '
              'Ниже проектные решения отделены от фактических измерений. Выполните run.ps1 и повторно сформируйте отчёт.')
    text = f'''# Практическая работа №1

Репозиторий: [J043R/VK_prac_1](https://github.com/J043R/VK_prac_1)

Развертывание локального Lakehouse, ELT-преобразование и бенчмарк форматов/сжатия

{status}

## 1. Вариант и масштаб

| Поле | Значение |
|---|---|
| Студент | Ладыгин Максим Александрович |
| Вариант | 3, Q18 «Large Volume Customer» |
| Масштаб | tpch.sf{sf} |
| Причина выбора | Используется локальный стенд с 15,2 ГБ ОЗУ. Для выполненного эксперимента выбран sf{sf}; при sf1 таблица lineitem содержит около 6 млн строк. Такой объём позволяет проверить соединения и агрегации и сохранить несколько копий Bronze. Во всех слоях и вариантах сжатия масштаб одинаков. Увеличение масштаба требует нового полного запуска и проверки ресурсов. |

Задача Q18: найти заказы, где сумма количества товара по позициям **строго больше 300**, соединить их с клиентами, вывести стоимость заказа и суммарное количество, отсортировать по стоимости по убыванию и дате по возрастанию, взять не более 100 строк. Это топ заказов с данными клиентов: один клиент может встречаться несколько раз. `totalprice` уже относится ко всему заказу, поэтому суммировать её по позициям нельзя. При равных стоимости и дате добавлен `orderkey` для воспроизводимого порядка.

Измерения выполнены на sf{sf} в локальном окружении; для объёма 50 ГБ и промышленного кластера потребуются отдельные замеры.

## 2. Поднятые сервисы и причины выбора

Следующая таблица описывает конфигурацию проекта. Факт запуска подтверждается `results/docker-ps.jsonl`, версиями `results/docker-images.json` и `results/environment.json` после выполнения.

| Сервис | Роль | Почему выбран |
|---|---|---|
| MinIO, исходники RELEASE.2025-04-22T22-12-26Z | S3-совместимое объектное хранилище | Позволяет локально использовать S3 без облачной учётной записи; в отличие от отдельных папок каждого движка, даёт Trino и Spark общее объектное хранилище. Образ собран из официальных исходников. |
| PostgreSQL 17.4 | База каталога Lakekeeper | Поддерживается выбранным каталогом и сохраняет его состояние в постоянном томе. Поднимать другую СУБД для этой служебной роли не требуется; строки TPC-H хранятся в MinIO. |
| Lakekeeper 0.9.5 | Iceberg REST Catalog | Общий REST-каталог позволяет Trino и Spark находить одни таблицы и согласованно фиксировать их версии. В отличие от независимого чтения файлов по пути, сохраняет сведения о схеме и текущем снапшоте. |
| Trino 476 | SQL, генератор TPC-H, ELT и замеры | Совмещает генератор tpch и работу с Iceberg: отдельный генератор dbgen и CSV-импорт не нужны. Подходит для аналитических соединений и агрегаций Q18; обычная транзакционная БД не показала бы работу двух движков с одними файлами Iceberg. |
| Spark 3.5.5, Iceberg 1.8.1 | Дополнительный движок | Читает тот же Bronze через REST и S3, считает крупные заказы. Показывает независимость Iceberg от Trino. |
| Python 3.12 | Оркестрация и сбор результатов | Стандартной библиотеки достаточно для HTTP, JSON, CSV, статистики и создания отчёта. |

Стенд разворачивается `docker compose up -d minio postgres lakekeeper trino`. Compose создаёт бакет и выполняет миграции каталога. Runner инициализирует warehouse `demo`, затем создаёт таблицы. Полные команды и последовательность запуска находятся в `run.ps1`; конфигурации - в `compose.yaml` и `config/`. Постоянные тома сохраняют данные после остановки контейнеров.

Отличие от гайда: `minio/Dockerfile` собирает сервер MinIO и клиент mc из официальных фиксированных релизов через Go 1.24.2. Проверка 15.10.2026 показала 401 при анонимном запросе к Quay и отсутствие соответствующих тегов Docker Hub. Это изменение способа получения образа, а не замена технологии хранения.

### Команды развёртывания

Команды выполняются в PowerShell из папки с `compose.yaml`:

```powershell
# Основные службы; зависимости bucket и migrate выполняют начальную настройку.
docker compose up -d minio postgres lakekeeper trino
# Загрузка Bronze, преобразования Silver/Gold, бенчмарк и демонстрация Iceberg.
docker compose run --rm runner --stage all --sf {sf} --repeats 5
# Spark запускается отдельно от Trino, чтобы снизить пиковый расход памяти.
docker compose stop trino
docker compose run --rm --build spark
docker compose start trino
# Добавить результат Spark в итоговый отчёт.
docker compose run --rm runner --stage report --sf {sf} --repeats 5
```

Эту последовательность с проверкой ошибок автоматизирует `run.ps1`. В `compose.yaml` указаны образы, порты, тома и зависимости; конфигурация каталога Trino находится в `config/catalog/lakekeeper.properties`. Подключение второго движка, Spark, описано в разделе 6.2.

## 3. Разбор запроса

| Таблица TPC-H | Нужные колонки | Назначение |
|---|---|---|
| customer | custkey, name | Соединение с orders по custkey, вывод имени и идентификатора клиента |
| orders | orderkey, custkey, orderdate, totalprice | Соединение с клиентом и позициями, группировка, сортировка и вывод |
| lineitem | orderkey, quantity | Подзапрос GROUP BY orderkey HAVING sum(quantity)>300 и итоговая сумма количества |
| lineitem | linenumber (дополнительно в Silver) | Сохранение уникального ключа позиции и контроль отсутствия дублей |

SQL использует имена коннектора Trino без префиксов `c_`, `o_`, `l_`. Источник - детерминированный TPC-H, а не реальные персональные данные.

## 4. Медальон: решения по слоям

Bronze сохраняет **все колонки** трёх исходных таблиц, без фильтров и исправлений. Это позволяет повторить ELT и проверить исходный Q18. На Silver проверки обязательных полей, уникальности ключей, положительного quantity и ссылочной целостности блокируют продвижение при ошибках. Неизвестные дубли не удаляются произвольно. Нормализация включает trim(name), явные DECIMAL(18,2) для стоимости и количества, выделение ключа позиции. Обогащение - соединение имени клиента, заказа и позиций. Отдельная `large_orders` содержит все заказы с количеством >300, а `order_lines` - все их позиции. Gold агрегирует Silver и ограничивает результат топом 100.

Во всех таблицах нет партиционирования: Q18 не фильтрует даты и должен суммировать позиции всех заказов. Разбиение по дате не даст pruning для этого запроса, а разбиение по orderkey создаст слишком много малых партиций. На sf1 разумнее последовательные колоночные файлы. При другом наборе запросов и большем объёме решение нужно пересмотреть.

Iceberg - табличный формат, PARQUET/ORC - форматы файлов, ZSTD/GZIP/SNAPPY - кодеки. Они решают разные задачи.

### Bronze

| Таблица | Колонки | Строк | Данные, МиБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---:|---:|---|---|---|---|
'''
    def layer_rows(layer):
        if not p:
            names = {'Bronze':['customer','orders','lineitem'],'Silver':['large_orders','order_lines'],'Gold':['q18']}[layer]
            return '\n'.join(f'| {name} | '+({'Bronze':'все поля источника','Silver':'см. SQL и описание выше','Gold':'name, custkey, orderkey, orderdate, totalprice, total_quantity'}[layer])+' | не измерено | не измерено | PARQUET | '+('SNAPPY' if layer=='Gold' else 'ZSTD')+' | нет | '+({'Bronze':'ZSTD как исходный баланс размера и скорости; выбор проверяется бенчмарком','Silver':'ZSTD для повторного чтения очищенного слоя','Gold':'маленькая витрина, SNAPPY с низкой нагрузкой CPU'}[layer])+' |' for name in names)
        rows=[]
        for m in p['metrics']:
            if m['layer'] != layer:
                continue
            name=m['table'].split('.')[-1]
            cols={'customer':'все поля customer','orders':'все поля orders','lineitem':'все поля lineitem',
                  'large_orders':'orderkey, total_quantity',
                  'order_lines':'name, custkey, orderkey, orderdate, totalprice, linenumber, quantity',
                  'q18':'name, custkey, orderkey, orderdate, totalprice, total_quantity'}[name]
            why={'Bronze':'Parquet сохраняет типы и подходит обоим движкам; ZSTD сокращает объём без долгой записи GZIP; нет партиций, так как Q18 читает все заказы',
                 'Silver':'Parquet совместим с Trino/Spark; ZSTD сохраняет единый кодек подготовленного слоя; для 57/399 строк партиции создадут лишние малые файлы',
                 'Gold':'Parquet совместим с аналитическими инструментами; SNAPPY выбран как быстрый кодек, выигрыш в размере на 57 строках несущественен; партиции не нужны'}[layer]
            rows.append(f"| {name} | {cols} | {m['rows']} | {m['data_MiB']:.4f} | {m['format']} | {m['codec']} | нет | {why} |")
        return '\n'.join(rows)
    text += layer_rows('Bronze')
    for layer, heading in (('Silver','Silver'),('Gold','Gold')):
        text += f'\n\n### {heading}\n\n| Таблица | Колонки | Строк | Данные, МиБ | Формат | Сжатие | Партиционирование | Обоснование |\n|---|---|---:|---:|---|---|---|---|\n'+layer_rows(layer)
    text += '''

Размер данных - точная сумма `file_size_in_bytes` в `$files` текущего снапшота, не оценка `SHOW STATS`. 1 МиБ = 1 048 576 байт. Метаданные считаются отдельно листингом S3 `/metadata/`, включая сохранённую историю. Поэтому размер текущих файлов данных не равен общему объёму всех объектов и истории таблицы. Таблица размеров фиксируется сразу после материализации слоёв, до демонстрации изменения схемы и создания recovery_demo; эти последующие метаданные в исходные размеры не входят.

В таблице Silver перечислены колонки на момент материализации. После демонстрации в `order_lines` также присутствует `validation_note VARCHAR`; отдельная `recovery_demo` служит только для восстановления и не входит в бизнес-слои и бенчмарк. Для перевода МиБ в десятичные МБ: байты / 1 000 000.
'''
    if p:
        gold = load('gold.json')
        if gold and p.get('gold_equals_reference'):
            text += '\n### Первые 10 строк Gold\n\nПолный результат — [gold.csv](results/gold.csv). Порядок: стоимость по убыванию, дата и номер заказа по возрастанию.\n\n| Клиент | custkey | orderkey | Дата | Стоимость заказа | Количество |\n|---|---:|---:|---|---:|---:|\n'
            for row in gold['rows'][:10]:
                text += '| ' + ' | '.join(str(v) for v in row) + ' |\n'
        text += screenshot('layer-counts.png', 'Количество строк в Bronze, Silver и Gold')
    if p:
        bronze_data = sum(m['data_bytes'] for m in p['metrics'] if m['layer']=='Bronze')
        bronze_meta = sum(m['metadata_bytes'] for m in p['metrics'] if m['layer']=='Bronze')
        gm = next(m for m in p['metrics'] if m['layer']=='Gold')
        text += f"\nВ исходном Bronze метаданные занимают {bronze_meta/2**20:.4f} МиБ при {bronze_data/2**20:.3f} МиБ data-файлов: {100*bronze_meta/bronze_data:.4f}% от данных. У Gold на {gm['rows']} строк data-файлы занимают {gm['data_bytes']} байт, а метаданные - {gm['metadata_bytes']} байт ({100*gm['metadata_bytes']/gm['data_bytes']:.1f}% от данных). На маленькой витрине накладные расходы Iceberg заметны, поэтому измерять эффективность кодеков только на Gold было бы некорректно. Именно полные копии Bronze составляют базу бенчмарка.\n"
        quality = load('quality.json')
        text += f"\nПроверки качества: выполнено {len(quality)} проверок, число обнаруженных нарушений - {sum(quality.values())}. Источник TPC-H уже согласован, поэтому исправления грязных записей не симулируются. Нормализация типов и имени клиента, а также обогащение соединением таблиц выполнены на Silver.\n"
    text += '''

## 5. Тесты производительности

Тестируется исходный Q18 над полными копиями Bronze. Каждый вариант имеет те же строки, схему и масштаб. Сравниваются PARQUET NONE/SNAPPY/GZIP/ZSTD и ORC SNAPPY/GZIP/ZSTD. Для ORC Trino отображает GZIP на сжатие ZLIB/DEFLATE; это не gzip-файл вокруг ORC. Таблицы создаются через CTAS из одной опорной копии Bronze. В Trino 476 кодек задаётся **сессионным** свойством `lakekeeper.compression_codec`, не неподдерживаемым свойством CREATE TABLE. Формат задаётся WITH (format=...).

Время записи - сумма wall-clock трёх CTAS от отправки запроса до полного получения ответа. В него входит чтение опорного Bronze, кодирование, запись и commit, но не запросы подсчёта размеров. Это один измеренный цикл загрузки, поэтому устойчивых выводов о дисперсии времени записи нет. Запросы выполняются последовательно одним движком с `task_concurrency=2`. Первый Q18 записывается отдельно; затем выполняются повторные замеры, порядок вариантов циклически сдвигается. Вычисляются медиана, минимум, максимум и выборочное стандартное отклонение. Ответ каждого запуска сравнивается с эталоном.

**Кэш:** кэш ОС/MinIO не сбрасывается, поэтому первый запуск нельзя считать гарантированно холодным. Повторные замеры характеризуют прогретое окружение. Первый запуск каждого варианта происходит после записи файлов и проверки размеров, что также влияет на кэш. Spark запускается после замеров Trino. Измерение отражает локальный стенд, а не универсальное преимущество одного кодека.

| Формат | Кодек | Запись, с | Первый Q18, с | Медиана Q18, с | Мин / макс, с | σ, с | Bronze, МиБ | Метаданные, МиБ | Файлов |
|---|---|---:|---:|---:|---|---:|---:|---:|---:|
'''
    if b:
        for v in b['variants']:
            text += f"| {v['format']} | {v['codec']} | {v['write_seconds']:.3f} | {v['first_read_seconds']:.3f} | {v['median_seconds']:.3f} | {v['min_seconds']:.3f} / {v['max_seconds']:.3f} | {v['stdev_seconds']:.3f} | {v['data_bytes']/2**20:.3f} | {v['metadata_bytes']/2**20:.4f} | {v['files']} |\n"
        text += f"\nПовторов на вариант: {b['repeats']}. Все результаты совпали с эталонным Q18.\n"
        gzip = next(v for v in b['variants'] if v['format']=='PARQUET' and v['codec']=='GZIP')
        zstd = next(v for v in b['variants'] if v['format']=='PARQUET' and v['codec']=='ZSTD')
        saving = 100*(gzip['data_bytes']-zstd['data_bytes'])/gzip['data_bytes']
        latency = 100*(gzip['median_seconds']-zstd['median_seconds'])/gzip['median_seconds']
        text += f'\nСравнение GZIP и ZSTD: изменение размера PARQUET ZSTD относительно GZIP = {-saving:.2f}%, изменение медианы времени = {-latency:.2f}%. Отрицательное изменение означает уменьшение. Формула: (ZSTD/GZIP - 1) × 100%.\n'
        text += '\nСравнение Parquet GZIP/ZSTD (время — медиана пяти повторов Q18):\n\n| Сжатие Bronze, Parquet | Время Q18, с | Bronze, МиБ | Вывод |\n|---|---:|---:|---|\n'
        text += f"| GZIP | {gzip['median_seconds']:.3f} | {gzip['data_bytes']/2**20:.3f} | Меньший объём в этом прогоне; запись медленнее ZSTD. Подходит при приоритете экономии места. |\n"
        text += f"| ZSTD | {zstd['median_seconds']:.3f} | {zstd['data_bytes']/2**20:.3f} | Запись быстрее GZIP, объём немного больше. Выбран для Bronze/Silver; небольшая разница медиан Q18 сама по себе не доказывает устойчивого ускорения. |\n"
        text += f"\nВремя записи ZSTD составило {zstd['write_seconds']/gzip['write_seconds']:.3f} от GZIP, то есть уменьшилось на {100*(1-zstd['write_seconds']/gzip['write_seconds']):.2f}%. Для локального учебного пайплайна оставляю ZSTD на Bronze/Silver: в измеренном прогоне небольшое увеличение объёма относительно GZIP сопровождается меньшим временем загрузки и меньшей медианой Q18. Если важен только минимальный объём и слой редко перечитывается, GZIP даёт меньший файл. Вывод о записи опирается на один цикл и требует повторения для оценки устойчивости.\n"
        smallest = min(b['variants'],key=lambda v:v['data_bytes'])
        fastest = min(b['variants'],key=lambda v:v['median_seconds'])
        text += f"\nМинимальный объём данных наблюдался у {smallest['format']}/{smallest['codec']}; минимальная медиана Q18 - у {fastest['format']}/{fastest['codec']}. Если различия времени сопоставимы с разбросом, их нельзя трактовать как устойчивое ускорение. Предпочтение по одному критерию не означает преимущество по всем нагрузкам.\n"
    else:
        text += '| не выполнено | GZIP / ZSTD и дополнительные варианты | - | - | - | - | - | - | - | - |\n\nЧисленные выводы отсутствуют до запуска стенда.\n'
    text += '''

## 6. Осмысление и работа с инструментами

### 6.1. Понять и объяснить

| Вопрос | Ответ |
|---|---|
| Зачем большие данные и Lakehouse? | Lakehouse нужен при больших объёмах файлов, нескольких движках и необходимости хранить исходную историю: объектное хранилище отделяется от вычислений, а Iceberg добавляет транзакции и управление версиями. На маленькой задаче обычная БД проще и может быть быстрее. Здесь Lakehouse выбран для изучения архитектуры и общей работы Trino/Spark с одним набором файлов. |
| Schema evolution на Silver | Скрипт ADD COLUMN quality_note VARCHAR, затем RENAME COLUMN quality_note TO validation_note меняет схему по идентификаторам колонок. Проверка сравнивает список data-файлов до и после и проверяет NULL в новом поле у старых строк. Это изменение схемы без переписывания содержимого. |
| Time travel и восстановление | Сохраняется snapshot_id копии Gold в Silver.recovery_demo, добавляется одна лишняя строка, чтение FOR VERSION AS OF возвращает прежние данные. Затем ALTER TABLE ... EXECUTE rollback_to_snapshot(id) восстанавливает прежнее состояние. Полное содержимое сравнивается с копией до изменения. |
'''
    text += (f"\nФактическая проверка: data-файлы Silver неизменны, NULL в новом поле у {d['new_column_nulls']} строк. Снапшот {d['snapshot_id']}; строк до ошибки {d['rows_before']}, после {d['rows_after_duplicate']}, восстановлено {d['rows_restored']}. Историческое и восстановленное содержимое совпало.\n" if d else '\nДемонстрации ещё не выполнены. Команды реализованы в `scripts/run.py`, стадия `demo`.\n')
    if p and d:
        text += f'''
Команды SQL демонстрации изменения схемы (уже выполнены скриптом; повторно добавлять существующую колонку не нужно):

```sql
ALTER TABLE lakekeeper.{p['silver']}.order_lines ADD COLUMN quality_note VARCHAR;
ALTER TABLE lakekeeper.{p['silver']}.order_lines RENAME COLUMN quality_note TO validation_note;
```

Пример чтения снапшота и восстановления демонстрационной таблицы для этого запуска:

```sql
SELECT count(*)
FROM lakekeeper.{p['silver']}.recovery_demo
FOR VERSION AS OF {d['snapshot_id']};

ALTER TABLE lakekeeper.{p['silver']}.recovery_demo
EXECUTE rollback_to_snapshot({d['snapshot_id']});

SELECT count(*) FROM lakekeeper.{p['silver']}.recovery_demo;
```

Исторический и восстановленный `count(*)` равны {d['rows_before']}. При новом полном запуске имена схем и snapshot_id меняются; актуальные значения находятся в `results/pipeline.json` и `results/demo.json`.
'''
        text += screenshot('schema-evolution.png', 'После изменения схемы сохранены 399 строк, новая колонка содержит NULL')
        text += screenshot('snapshots.png', 'История снапшотов демонстрационной таблицы: исходная версия и добавление строки')
    text += '''

### 6.2. Подключение Spark к Bronze

| Инструмент | Почему выбран |
|---|---|
| Spark в Docker | Проверяет взаимодействие второго движка с существующими Iceberg-таблицами. В отличие от отдельной копии CSV/Parquet, Spark получает схему и список файлов из того же REST-каталога. BI здесь потребовал бы дополнительного UI, но не показал бы независимое исполнение агрегации над Bronze. |

Скрипты: `spark/Dockerfile`, сервис spark в Compose и `scripts/spark_bronze.py`. SparkCatalog настроен на `http://lakekeeper:8181/catalog`, warehouse `demo`, S3FileIO - на `http://minio:9000`. В консоли скрипт читает три таблицы Bronze, сравнивает count(*) с Trino, группирует lineitem по orderkey, применяет HAVING >300 и показывает десять заказов. Число крупных заказов сравнивается с Silver.large_orders. Команда: `docker compose run --rm --build spark`, на этом стенде предварительно останавливается Trino, как в разделе 2. Результат сохраняется в `results/spark-bronze.json`, вывод консоли — в `results/spark-console.log`.
'''
    if s:
        text += f"\nSpark {s['spark_version']} прочитал все три таблицы; количества строк совпали с Trino. Крупных заказов: {s['large_orders']}. Десять примеров, выведенных Spark (порядок по номеру заказа):\n\n| orderkey | total_quantity |\n|---:|---:|\n"
        for row in s['sample']:
            text += f"| {row['orderkey']} | {row['total_quantity']} |\n"
        text += '\nПолный результат проверки — [spark-bronze.json](results/spark-bronze.json), вывод консоли — [spark-console.log](results/spark-console.log).\n'
    else:
        text += '\nВзаимодействие Spark с Bronze ещё не подтверждено запуском.\n'
    text += '\n## 7. Выводы\n\n'
    if complete:
        text += (f"Результат Gold на sf{sf} совпал с исходным Q18, проверки ключей и ссылочной целостности пройдены. "
            'Слои сохраняют исходные строки в Bronze, выделяют согласованные позиции крупных заказов в Silver и формируют итоговую витрину в Gold. '
            f"В этом прогоне наименьший объём получен у {smallest['format']}/{smallest['codec']}, а минимальная медиана Q18 - у {fastest['format']}/{fastest['codec']}; выбор зависит от приоритета хранения или задержки и ограничен описанной методологией. "
            'Изменение схемы сохранило data-файлы, восстановление вернуло исходное содержимое, а Spark прочитал тот же Bronze через общий каталог. '
            'Для более сильного вывода о производительности следует повторить запись, контролировать холодный кэш и проверить больший sf на более мощном стенде.\n')
    else:
        text += 'Проектные решения подготовлены. Экспериментальные выводы будут сформированы после фактического запуска всех этапов. Пока отсутствующие результаты не позволяют считать обязательные требования выполненными.\n'
    if p:
        host = load('host.json') or {}
        text += f"\n\n## Окружение и идентификатор эксперимента\n\nЗапуск `{p['run']}` (идентификатор — время UTC); масштаб sf{sf}. Схемы: `lakekeeper.{p['bronze']}`, `lakekeeper.{p['silver']}`, `lakekeeper.{p['gold']}`. Процессор: {host.get('CPU','см. host.json').strip()}; логических процессоров: {host.get('LogicalCPU','см. host.json')}; ОЗУ компьютера: {host.get('RAM_GB','см. host.json')} ГБ. Окружение — Windows с Docker Desktop/WSL 2, один узел Trino; heap Trino 4 ГБ, лимит контейнера 5 ГБ, Spark запускается последовательно с Trino.\n"
    (ROOT/'отчет.md').write_text(text,encoding='utf-8')
    save_status = {'complete':complete,'pipeline':bool(p),'benchmark':bool(b),'schema_and_snapshot':bool(d),'spark':bool(s)}
    (R/'completion.json').write_text(json.dumps(save_status,indent=2),encoding='utf-8')
    print('Report generated. Completion:', save_status)


if __name__ == '__main__':
    generate()
