from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import delta-spark
print(delta-spark.__version__)


# Initialize Spark Session with Delta support
spark = SparkSession.builder.appName("DeltaLakeACID") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define Delta table path
delta_table_path = "C:/Users/LENOVO/OneDrive/spark/Spark_tutorials/target"

# Create a Delta table (if not exists)
data = [(1, "Alice", 1000), (2, "Bob", 1500)]
df = spark.createDataFrame(data, ["id", "name", "balance"])
df.write.format("delta").mode("overwrite").save(delta_table_path)

delta_table = DeltaTable.forPath(spark, delta_table_path)

# ACID Transaction: Atomic Upsert (MERGE)
update_df = spark.createDataFrame([(2, "Bob", 2000)], ["id", "name", "balance"])
delta_table.alias("target").merge(
    update_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(set={"balance": "source.balance"}) \
 .whenNotMatchedInsert(values={"id": "source.id", "name": "source.name", "balance": "source.balance"}) \
 .execute()

# ACID Transaction: Ensuring Isolation with Optimistic Concurrency Control
try:
    delta_table.update(
        condition="id = 1",
        set={"balance": "balance + 500"}
    )
except Exception as e:
    print(f"Transaction Failed: {e}")

# Enable Time Travel (Retrieve old data version)
old_version_df = spark.read.format("delta").option("versionAsOf", 1).load(delta_table_path)
old_version_df.show()

# Implementing Explicit Transactions (Databricks Only)
spark.sql("BEGIN TRANSACTION")
spark.sql("UPDATE delta.`/mnt/delta/accounts` SET balance = balance - 500 WHERE id = 1")
spark.sql("UPDATE delta.`/mnt/delta/accounts` SET balance = balance + 500 WHERE id = 2")
spark.sql("COMMIT")
