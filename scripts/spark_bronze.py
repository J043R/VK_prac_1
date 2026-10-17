"""Read the same Bronze Iceberg tables through Spark REST catalog."""
import json
from pathlib import Path
import time
from pyspark.sql import SparkSession

root = Path('/project')
state = json.loads((root/'results/pipeline.json').read_text())
spark = (SparkSession.builder.appName('Ladygin-PR1-Bronze')
    .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.lake','org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.lake.type','rest')
    .config('spark.sql.catalog.lake.uri','http://lakekeeper:8181/catalog')
    .config('spark.sql.catalog.lake.warehouse','demo')
    .config('spark.sql.catalog.lake.io-impl','org.apache.iceberg.aws.s3.S3FileIO')
    .config('spark.sql.catalog.lake.s3.endpoint','http://minio:9000')
    .config('spark.sql.catalog.lake.s3.path-style-access','true')
    .config('spark.sql.catalog.lake.s3.access-key-id','minioadmin')
    .config('spark.sql.catalog.lake.s3.secret-access-key','minioadmin')
    .config('spark.sql.catalog.lake.client.region','us-east-1')
    .config('spark.sql.shuffle.partitions','8')
    .getOrCreate())
try:
    output = {'run':state['run'],'spark_version':spark.version,'tables':[]}
    for table in ('customer','orders','lineitem'):
        df = spark.table(f"lake.{state['bronze']}.{table}")
        count = df.count()
        expected = next(m['rows'] for m in state['metrics'] if m['layer']=='Bronze' and m['table'].endswith('.'+table))
        assert count == expected, f'Spark/Trino count mismatch: {table}'
        output['tables'].append({'table':table,'rows':count,'schema':df.schema.jsonValue()})
    from pyspark.sql import functions as F
    start = time.perf_counter()
    lines = spark.table(f"lake.{state['bronze']}.lineitem")
    big = lines.groupBy('orderkey').agg(F.sum('quantity').alias('total_quantity')).filter('total_quantity > 300')
    count_big = big.count()
    sample = [r.asDict() for r in big.orderBy('orderkey').limit(10).collect()]
    output.update(large_orders=count_big, sample=sample, wall_seconds=time.perf_counter()-start,
                  all_counts_match=True)
    expected_big = next(m['rows'] for m in state['metrics'] if m['table'].endswith('.large_orders'))
    assert count_big == expected_big, 'Spark/Trino large order count mismatch'
    (root/'results/spark-bronze.json').write_text(json.dumps(output,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    print(json.dumps(output,ensure_ascii=False,indent=2,default=str))
finally:
    spark.stop()
