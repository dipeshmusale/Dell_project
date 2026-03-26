#Final script 

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, input_file_name
from pyspark.sql.types import StructType

# =========================
# INIT
# =========================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.parquet.mergeSchema", "true")

# =========================
# PATHS
# =========================
csv_path = "s3://process-s3-data/raw/csv/"
xml_path = "s3://process-s3-data/raw/xml/"
output_path = "s3://process-s3-data/processed/final/"

# =========================
# 1. READ CSV (DYNAMIC)
# =========================
csv_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("mode", "PERMISSIVE") \
    .csv(csv_path)

# Clean column names
for c in csv_df.columns:
    new_col = c.strip().replace(" ", "_").replace(".", "_")
    csv_df = csv_df.withColumnRenamed(c, new_col)

# Add metadata
csv_df = csv_df.withColumn("source", lit("csv")) \
               .withColumn("file_name", input_file_name())

# =========================
# 2. READ XML (DYNAMIC)
# =========================
xml_df = spark.read.format("xml") \
    .option("rowTag", "MatchListEntry") \
    .option("inferSchema", "true") \
    .load(xml_path)

# Flatten function
def flatten_df(df):
    flat_cols = []
    nested_cols = []

    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            nested_cols.append(field.name)
        else:
            flat_cols.append(field.name)

    flat_df = df.select(flat_cols + [
        col(nc + "." + c).alias(nc + "_" + c)
        for nc in nested_cols
        for c in df.select(nc + ".*").columns
    ])

    if len(nested_cols) != 0:
        return flatten_df(flat_df)
    else:
        return flat_df

xml_df = flatten_df(xml_df)

# Clean column names
for c in xml_df.columns:
    new_col = c.replace(".", "_").replace(" ", "_")
    xml_df = xml_df.withColumnRenamed(c, new_col)

# Add metadata
xml_df = xml_df.withColumn("source", lit("xml")) \
               .withColumn("file_name", input_file_name())

# =========================
# 3. ALIGN SCHEMA (DYNAMIC)
# =========================
all_columns = list(set(csv_df.columns).union(set(xml_df.columns)))

def align_schema(df, columns):
    for c in columns:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))
    return df.select(columns)

csv_df = align_schema(csv_df, all_columns)
xml_df = align_schema(xml_df, all_columns)

# =========================
# 4. UNION BOTH
# =========================
final_df = csv_df.unionByName(xml_df, allowMissingColumns=True)

# =========================
# 5. WRITE FINAL OUTPUT
# =========================
final_df.write \
    .mode("append") \
    .partitionBy("source", "CountryData") \
    .parquet(output_path)  


# I need to same changes in this script and i want to push in main branch
# I hope all this thing will work properly in production env

print("hello word")


