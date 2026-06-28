from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp, when, regexp_replace, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, IntegerType

meta_schema = StructType([
    StructField("uri", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("id", StringType(), True),
    StructField("dt", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("stream", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", LongType(), True),
    StructField("offset", LongType(), True)
])

length_schema = StructType([
    StructField("old", IntegerType(), True),
    StructField("new", IntegerType(), True)
])

revision_schema = StructType([
    StructField("old", LongType(), True),
    StructField("new", LongType(), True)
])

recentchange_schema = StructType([
    StructField("meta", meta_schema, True),
    StructField("id", LongType(), True),
    StructField("type", StringType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("minor", BooleanType(), True),
    StructField("patrolled", BooleanType(), True),
    StructField("length", length_schema, True),
    StructField("revision", revision_schema, True),
    StructField("server_url", StringType(), True),
    StructField("server_name", StringType(), True),
    StructField("server_script_path", StringType(), True),
    StructField("wiki", StringType(), True),
    StructField("parsedcomment", StringType(), True)
])

def parse_kafka_stream(df):
    return df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), recentchange_schema).alias("data")) \
        .select("data.*")

def clean_and_transform(df):
    parsed_df = parse_kafka_stream(df)
    
    namespace_name_expr = when(col("namespace") == 0, "Article") \
        .when(col("namespace") == 1, "Talk") \
        .when(col("namespace") == 2, "User") \
        .when(col("namespace") == 3, "User Talk") \
        .when(col("namespace") == 4, "Project") \
        .when(col("namespace") == 6, "File") \
        .when(col("namespace") == 14, "Category") \
        .otherwise("Other")
        
    lang_expr = regexp_replace(col("wiki"), "wiki$", "")
    
    page_url_expr = concat(col("server_url"), lit("/wiki/"), col("title"))
    
    change_size_expr = col("length.new") - col("length.old")
    
    change_category_expr = when(change_size_expr > 1000, "Major Addition") \
        .when(change_size_expr < -1000, "Major Deletion") \
        .when(change_size_expr == 0, "No Text Change") \
        .otherwise("Minor Edit")
        
    is_vandalism_warning_expr = when((col("bot") == False) & (change_size_expr < -5000), True).otherwise(False)
    
    return parsed_df.select(
        col("id"),
        col("wiki"),
        lang_expr.alias("language"),
        col("type"),
        col("namespace"),
        namespace_name_expr.alias("namespace_name"),
        col("title"),
        page_url_expr.alias("page_url"),
        col("user"),
        col("bot"),
        col("minor"),
        col("patrolled"),
        col("length.old").alias("length_old"),
        col("length.new").alias("length_new"),
        change_size_expr.alias("change_size"),
        change_category_expr.alias("change_category"),
        is_vandalism_warning_expr.alias("is_vandalism_warning"),
        col("revision.old").alias("revision_old"),
        col("revision.new").alias("revision_new"),
        col("server_name"),
        col("meta.dt").alias("meta_dt"),
        to_timestamp(from_unixtime(col("timestamp"))).alias("event_timestamp")
    )
