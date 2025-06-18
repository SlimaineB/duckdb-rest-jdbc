import time
from pyspark.sql import SparkSession

def benchmark_spark(df):
    start = time.time()
    result = df.groupBy("nom", "annee") \
               .avg("montant") \
               .withColumnRenamed("avg(montant)", "moyenne_montant") \
               .orderBy("nom", "annee")
    result.orderBy("nom", "annee").show(10)
    duration = time.time() - start
    print(f"Temps d'exécution lecture + agrégation Spark natif : {duration:.2f} sec")
    return duration

def benchmark_duckdb(spark, jdbc_url, query, driver):
    start = time.time()
    df_duckdb = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", query) \
        .option("driver", driver) \
        .option("partitionColumn", "annee") \
        .option("lowerBound", "2015") \
        .option("upperBound", "2024") \
        .option("numPartitions", "10") \
        .load()
    df_duckdb.orderBy("nom", "annee").show(10)
    duration = time.time() - start
    print(f"Temps d'exécution lecture + agrégation DuckDB via JDBC : {duration:.2f} sec")
    return duration

def main():
    spark = SparkSession.builder \
        .appName("SparkVsDuckDBBenchmark") \
        .getOrCreate()

    # Config MinIO pour accès S3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Lecture brute Parquet S3 (mêmes fichiers que DuckDB)
    parquet_path = "s3a://test-bucket/parquet_partitionne_big/region=Est/"
    print("Lecture brute Parquet S3 (Spark)...")
    df_s3 = spark.read.parquet(parquet_path)

    time_spark = benchmark_spark(df_s3)

    # Lecture + agrégation via DuckDB JDBC
    print("\nLecture + agrégation via DuckDB JDBC...")

    jdbc_url = "jdbc:duckdb://localhost:8080"
    driver = "com.slim.duckdb.DuckDBDriver"

    # Requête SQL exécutée côté DuckDB : agrégation incluse
    query = """
    (
        SELECT nom, annee, AVG(montant) AS moyenne_montant
        FROM read_parquet('s3://test-bucket/parquet_partitionne_big/region=Est/annee=*/*.parquet')
        GROUP BY nom, annee
        ORDER BY nom, annee
    ) AS agg_parquet_table
    """

    time_duckdb = benchmark_duckdb(spark, jdbc_url, query, driver)

    print("\n=== Résumé du benchmark ===")
    print(f"Spark natif (lecture + agrégation) : {time_spark:.2f} sec")
    print(f"DuckDB JDBC (lecture + agrégation) : {time_duckdb:.2f} sec")

    spark.stop()

if __name__ == "__main__":
    main()
