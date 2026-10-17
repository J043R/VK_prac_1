"""PR1: standard-library runner. Run inside the Compose network."""
import argparse
import csv
import datetime as dt
import decimal
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import sys
import time
import traceback
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / 'results'
URL = os.environ.get('TRINO_URL', 'http://trino:8080')
CATALOG = 'lakekeeper'
TABLES = ('customer', 'orders', 'lineitem')
COMBOS = (('PARQUET', 'NONE'), ('PARQUET', 'SNAPPY'), ('PARQUET', 'GZIP'),
          ('PARQUET', 'ZSTD'), ('ORC', 'SNAPPY'), ('ORC', 'GZIP'), ('ORC', 'ZSTD'))


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')


def request(url, data=None, headers=None, method=None):
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    with urllib.request.urlopen(req, timeout=120) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def wait(url, timeout=240):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            value = request(url)
            if not isinstance(value, dict) or not value.get('starting', False):
                return
        except (OSError, ValueError):
            pass
        time.sleep(2)
    raise TimeoutError(f'Service not ready: {url}')


class Trino:
    def __init__(self, log):
        self.log = log

    def query(self, sql, codec='ZSTD', label='query'):
        headers = {'X-Trino-User': 'ladygin', 'X-Trino-Source': 'pr1-reproducible',
                   'X-Trino-Session': f'lakekeeper.compression_codec={codec},task_concurrency=2'}
        started = time.perf_counter()
        page = request(URL + '/v1/statement', sql.encode('utf-8'), headers)
        rows, columns = [], []
        while True:
            if 'error' in page:
                save(self.log / f'error-{time.time_ns()}.json', {'sql': sql, 'response': page})
                raise RuntimeError(page['error'].get('message', str(page['error'])))
            if 'columns' in page:
                columns = [c['name'] for c in page['columns']]
            rows.extend(page.get('data', []))
            if not page.get('nextUri'):
                break
            page = request(page['nextUri'], headers=headers)
        result = {'id': page.get('id'), 'label': label, 'sql': sql, 'codec': codec,
                  'wall_seconds': time.perf_counter() - started, 'stats': page.get('stats', {}),
                  'columns': columns, 'rows': rows}
        save(self.log / f'{time.time_ns()}-{label}.json', result)
        print(f"{label}: {result['wall_seconds']:.3f}s", flush=True)
        return result

    def scalar(self, sql, **kwargs):
        return self.query(sql, **kwargs)['rows'][0][0]


def q18(schema):
    # Extra orderkey only resolves ties without changing teacher's ranking.
    return f'''SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice,
    sum(l.quantity) AS total_quantity
    FROM {schema}.customer c
    JOIN {schema}.orders o ON c.custkey=o.custkey
    JOIN {schema}.lineitem l ON o.orderkey=l.orderkey
    WHERE o.orderkey IN (SELECT orderkey FROM {schema}.lineitem
      GROUP BY orderkey HAVING sum(quantity)>300)
    GROUP BY c.name,c.custkey,o.orderkey,o.orderdate,o.totalprice
    ORDER BY o.totalprice DESC,o.orderdate,o.orderkey LIMIT 100'''


def canonical(rows):
    """TPC-H monetary and quantity columns have two fractional digits."""
    out = []
    for row in rows:
        name, cust, order, date, price, quantity = row
        out.append((name.strip(), int(cust), int(order), str(date),
                    str(decimal.Decimal(str(price)).quantize(decimal.Decimal('.01'))),
                    str(decimal.Decimal(str(quantity)).quantize(decimal.Decimal('.01')))))
    return sorted(out)


def require_equal(a, b, message):
    if canonical(a) != canonical(b):
        raise AssertionError(message)


def bootstrap():
    wait('http://lakekeeper:8181/health')
    base = 'http://lakekeeper:8181/management/v1/'
    headers = {'Content-Type': 'application/json'}
    try:
        request(base + 'bootstrap', b'{"accept-terms-of-use":true}', headers)
    except urllib.error.HTTPError as err:
        body = err.read().decode()
        if 'CatalogAlreadyBootstrapped' not in body and 'not open for bootstrap' not in body:
            raise RuntimeError(body) from err
    warehouses = request(base + 'warehouse?project-id=00000000-0000-0000-0000-000000000000')
    if any(w.get('warehouse-name') == 'demo' for w in warehouses.get('warehouses', [])):
        return
    spec = {'warehouse-name': 'demo', 'project-id': '00000000-0000-0000-0000-000000000000',
            'storage-profile': {'type': 's3', 'bucket': 'warehouse', 'key-prefix': 'pr1',
             'endpoint': 'http://minio:9000', 'region': 'us-east-1', 'path-style-access': True,
             'flavor': 'minio', 'sts-enabled': False},
            'storage-credential': {'type': 's3', 'credential-type': 'access-key',
             'aws-access-key-id': 'minioadmin', 'aws-secret-access-key': 'minioadmin'}}
    request(base + 'warehouse', json.dumps(spec).encode(), headers)


def metrics(db, schema, table):
    full = f'{CATALOG}.{schema}.{table}'
    # $files enumerates CURRENT snapshot, never table stats estimates.
    result = db.query(f'''SELECT count(*) AS files, coalesce(sum(record_count),0) AS records,
      coalesce(sum(file_size_in_bytes),0) AS bytes FROM {CATALOG}.{schema}."{table}$files"''', label='files')
    count = db.scalar(f'SELECT count(*) FROM {full}', label='count')
    nfiles, records, nbytes = result['rows'][0]
    if int(records) != int(count):
        raise AssertionError(f'File records vs SELECT count mismatch: {full}')
    value = {'table': full, 'rows': count, 'files': nfiles, 'data_bytes': nbytes,
            'data_MiB': nbytes / 2**20,
            'file_paths': db.query(f'SELECT file_path FROM {CATALOG}.{schema}."{table}$files"', label='paths')['rows']}
    from storage import storage_metrics
    value.update(storage_metrics(value['file_paths']))
    return value


def new_table(db, schema, table, select, fmt='PARQUET', codec='ZSTD'):
    return db.query(f"CREATE TABLE {CATALOG}.{schema}.{table} WITH (format='{fmt}',format_version=2) AS {select}",
                    codec=codec, label='create-' + table)


def pipeline(db, sf, run):
    bronze, silver, gold = [f'{layer}_{run}' for layer in ('bronze', 'silver', 'gold')]
    for schema in (bronze, silver, gold):
        db.query(f'CREATE SCHEMA {CATALOG}.{schema}', label='schema')
    layer_metrics = []
    for table in TABLES:
        result = new_table(db, bronze, table, f'SELECT * FROM tpch.sf{sf}.{table}')
        m = metrics(db, bronze, table)
        m.update(layer='Bronze', format='PARQUET', codec='ZSTD', write_s=result['wall_seconds'])
        layer_metrics.append(m)
        expected = db.scalar(f'SELECT count(*) FROM tpch.sf{sf}.{table}', label='source-count')
        if expected != m['rows']:
            raise AssertionError('Incomplete Bronze ingestion')

    # Fail closed instead of silently discarding inconsistent source records.
    checks = {
        'null_customer_keys': f'SELECT count(*) FROM {CATALOG}.{bronze}.customer WHERE custkey IS NULL OR name IS NULL',
        'duplicate_customer_keys': f'SELECT count(*) FROM (SELECT custkey FROM {CATALOG}.{bronze}.customer GROUP BY custkey HAVING count(*)>1)',
        'invalid_orders': f'SELECT count(*) FROM {CATALOG}.{bronze}.orders WHERE orderkey IS NULL OR custkey IS NULL OR orderdate IS NULL OR totalprice IS NULL OR totalprice<0',
        'duplicate_order_keys': f'SELECT count(*) FROM (SELECT orderkey FROM {CATALOG}.{bronze}.orders GROUP BY orderkey HAVING count(*)>1)',
        'invalid_lines': f'SELECT count(*) FROM {CATALOG}.{bronze}.lineitem WHERE orderkey IS NULL OR linenumber IS NULL OR quantity IS NULL OR quantity<=0',
        'duplicate_lines': f'SELECT count(*) FROM (SELECT orderkey,linenumber FROM {CATALOG}.{bronze}.lineitem GROUP BY orderkey,linenumber HAVING count(*)>1)',
        'orphan_orders': f'SELECT count(*) FROM {CATALOG}.{bronze}.orders o LEFT JOIN {CATALOG}.{bronze}.customer c ON c.custkey=o.custkey WHERE c.custkey IS NULL',
        'orphan_lines': f'SELECT count(*) FROM {CATALOG}.{bronze}.lineitem l LEFT JOIN {CATALOG}.{bronze}.orders o ON o.orderkey=l.orderkey WHERE o.orderkey IS NULL',
    }
    quality = {key: db.scalar(sql, label='quality-' + key) for key, sql in checks.items()}
    save(RESULTS / 'quality.json', quality)
    if any(quality.values()):
        raise AssertionError(f'Data quality gate failed: {quality}')
    new_table(db, silver, 'large_orders', f'''SELECT orderkey,sum(quantity) AS total_quantity
       FROM {CATALOG}.{bronze}.lineitem GROUP BY orderkey HAVING sum(quantity)>300''')
    new_table(db, silver, 'order_lines', f'''SELECT trim(c.name) AS name,c.custkey,o.orderkey,
       o.orderdate,CAST(o.totalprice AS DECIMAL(18,2)) AS totalprice,l.linenumber,
       CAST(l.quantity AS DECIMAL(18,2)) AS quantity
       FROM {CATALOG}.{bronze}.customer c
       JOIN {CATALOG}.{bronze}.orders o ON c.custkey=o.custkey
       JOIN {CATALOG}.{bronze}.lineitem l ON l.orderkey=o.orderkey
       JOIN {CATALOG}.{silver}.large_orders big ON big.orderkey=o.orderkey''')
    new_table(db, gold, 'q18', f'''SELECT name,custkey,orderkey,orderdate,totalprice,sum(quantity) AS total_quantity
       FROM {CATALOG}.{silver}.order_lines
       GROUP BY name,custkey,orderkey,orderdate,totalprice
       ORDER BY totalprice DESC,orderdate,orderkey LIMIT 100''', codec='SNAPPY')
    for schema, tables, layer, codec in ((silver, ('large_orders','order_lines'),'Silver','ZSTD'),
                                           (gold, ('q18',),'Gold','SNAPPY')):
        for table in tables:
            m = metrics(db, schema, table)
            m.update(layer=layer, format='PARQUET', codec=codec)
            layer_metrics.append(m)
    reference = db.query(q18(f'tpch.sf{sf}'), label='q18-reference')['rows']
    got = db.query(f'SELECT name,custkey,orderkey,orderdate,totalprice,total_quantity FROM {CATALOG}.{gold}.q18 ORDER BY totalprice DESC,orderdate,orderkey', label='q18-gold')['rows']
    require_equal(reference, got, 'Gold differs from original Q18')
    if any(decimal.Decimal(str(r[5])) <= 300 for r in got):
        raise AssertionError('Q18 threshold broken')
    save(RESULTS / 'gold.json', {'columns':['name','custkey','orderkey','orderdate','totalprice','total_quantity'], 'rows':got})
    with (RESULTS / 'gold.csv').open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f); writer.writerow(['name','custkey','orderkey','orderdate','totalprice','total_quantity']); writer.writerows(got)
    state = {'run':run,'sf':sf,'bronze':bronze,'silver':silver,'gold':gold,
             'metrics':layer_metrics,'reference':reference,'gold_equals_reference':True}
    save(RESULTS / 'pipeline.json', state)
    return state


def benchmark(db, state, repeats):
    variants = []
    for fmt, codec in COMBOS:
        schema = f"bench_{state['run']}_{fmt.lower()}_{codec.lower()}"
        db.query(f'CREATE SCHEMA {CATALOG}.{schema}', label='bench-schema')
        writes, size, files, metadata = [], 0, 0, 0
        for table in TABLES:
            result = new_table(db, schema, table, f"SELECT * FROM {CATALOG}.{state['bronze']}.{table}", fmt, codec)
            m = metrics(db, schema, table)
            writes.append(result['wall_seconds']); size += m['data_bytes']; files += m['files']; metadata += m['metadata_bytes']
        query = q18(f'{CATALOG}.{schema}')
        # First read is recorded separately, not mislabelled as a cold OS cache.
        first = db.query(query, label='bench-first')
        require_equal(first['rows'], state['reference'], f'Wrong result for {fmt}/{codec}')
        variants.append({'format':fmt,'codec':codec,'schema':schema,'data_bytes':size,
                         'files':files,'metadata_bytes':metadata,'write_seconds':sum(writes),'write_table_seconds':writes,
                         'first_read_seconds':first['wall_seconds'],'runs':[]})
    # Rotated order reduces fixed sequence bias. No parallel workloads.
    for repetition in range(repeats):
        for i in range(len(variants)):
            v = variants[(i+repetition) % len(variants)]
            r = db.query(q18(f"{CATALOG}.{v['schema']}"), label=f'bench-repeat-{repetition+1}')
            require_equal(r['rows'], state['reference'], 'Codec changed query result')
            v['runs'].append({'repetition':repetition+1,'wall_seconds':r['wall_seconds'],
                              'query_id':r['id'],'stats':r['stats']})
    for v in variants:
        times = [r['wall_seconds'] for r in v['runs']]
        v.update(median_seconds=statistics.median(times), min_seconds=min(times),
                 max_seconds=max(times), stdev_seconds=statistics.stdev(times),
                 all_results_equal=True)
    save(RESULTS / 'benchmark.json', {'run':state['run'],'sf':state['sf'],'repeats':repeats,'variants':variants})


def demo(db, state):
    schema = state['silver']
    table = f'{CATALOG}.{schema}.order_lines'
    paths_before = db.query(f'SELECT file_path FROM {CATALOG}.{schema}."order_lines$files"', label='evolution-files-before')['rows']
    db.query(f'ALTER TABLE {table} ADD COLUMN quality_note VARCHAR', label='add-column')
    db.query(f'ALTER TABLE {table} RENAME COLUMN quality_note TO validation_note', label='rename-column')
    paths_after = db.query(f'SELECT file_path FROM {CATALOG}.{schema}."order_lines$files"', label='evolution-files-after')['rows']
    if sorted(paths_before) != sorted(paths_after):
        raise AssertionError('Schema evolution unexpectedly rewrote data files')
    nulls = db.scalar(f'SELECT count(*) FROM {table} WHERE validation_note IS NULL', label='new-column-nulls')
    count = db.scalar(f'SELECT count(*) FROM {table}', label='evolution-count')
    if nulls != count:
        raise AssertionError('New column must be NULL for existing rows')
    # Snapshot experiment uses a separate copy, never damages Gold.
    target = f'{CATALOG}.{schema}.recovery_demo'
    new_table(db, schema, 'recovery_demo', f"SELECT * FROM {CATALOG}.{state['gold']}.q18")
    old = db.scalar(f'SELECT snapshot_id FROM {CATALOG}.{schema}."recovery_demo$snapshots" ORDER BY committed_at DESC LIMIT 1', label='saved-snapshot')
    expected = db.query(f'SELECT * FROM {target}', label='recovery-baseline')['rows']
    db.query(f'INSERT INTO {target} SELECT * FROM {target} LIMIT 1', label='accidental-duplicate')
    after = db.scalar(f'SELECT count(*) FROM {target}', label='count-after-duplicate')
    historic = db.query(f'SELECT * FROM {target} FOR VERSION AS OF {old}', label='time-travel')['rows']
    require_equal(historic, expected, 'Time travel data mismatch')
    db.query(f'ALTER TABLE {target} EXECUTE rollback_to_snapshot({old})', label='rollback')
    restored = db.query(f'SELECT * FROM {target}', label='restored')['rows']
    require_equal(restored, expected, 'Snapshot restore mismatch')
    if expected and after != len(expected)+1:
        raise AssertionError('Snapshot experiment did not introduce one duplicate')
    save(RESULTS / 'demo.json', {'run':state['run'],'data_files_unchanged':True,
          'new_column_nulls':nulls,'silver_rows':count,'snapshot_id':old,
          'rows_before':len(expected),'rows_after_duplicate':after,'rows_restored':len(restored),
          'time_travel_equal':True,'restore_equal':True})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stage', choices=['all','pipeline','benchmark','demo','report'], default='all')
    parser.add_argument('--sf', type=int, default=1)
    parser.add_argument('--repeats', type=int, default=5)
    args = parser.parse_args()
    if args.sf < 1 or args.repeats < 3:
        parser.error('sf >= 1 and repeats >= 3 required')
    RESULTS.mkdir(exist_ok=True)
    run = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d%H%M%S')
    log = RESULTS / 'runs' / run
    log.mkdir(parents=True, exist_ok=False)
    try:
        if args.stage in ('all','pipeline'):
            bootstrap()
        wait(URL + '/v1/info')
        db = Trino(log)
        if args.stage in ('all','pipeline'):
            save(RESULTS / 'environment.json', {'run':run,'utc':dt.datetime.now(dt.timezone.utc).isoformat(),
                 'platform':platform.platform(),'cpu_count':os.cpu_count(),'trino':request(URL+'/v1/info'),
                 'nodes':db.query('SELECT node_id,node_version,coordinator,state FROM system.runtime.nodes',label='nodes')['rows'],
                 'session':db.query('SHOW SESSION',label='session')['rows']})
            state = pipeline(db, args.sf, run)
        else:
            state = json.loads((RESULTS / 'pipeline.json').read_text(encoding='utf-8'))
            if state['sf'] != args.sf:
                raise ValueError('SF differs from existing pipeline; run pipeline first')
        if args.stage in ('all','benchmark'):
            benchmark(db, state, args.repeats)
        if args.stage in ('all','demo'):
            demo(db, state)
        if args.stage in ('all','report'):
            from report import generate
            generate()
    except Exception:
        (log / 'FAILED.txt').write_text(traceback.format_exc(), encoding='utf-8')
        raise


if __name__ == '__main__':
    main()
