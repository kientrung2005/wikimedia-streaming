import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)
sys.path.append(current_dir)

from config.spark_config import SparkConnect
from config.kafka_config import get_kafka_config
from spark_transform import clean_and_transform

def run():
    spark_conn = SparkConnect("WikimediaStructuredStreaming")
    spark = spark_conn.spark
    kafka_cfg = get_kafka_config()
    
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_cfg.bootstrap_servers) \
            .option("subscribe", kafka_cfg.topic) \
            .option("startingOffsets", "latest") \
            .load()
            
        transformed_df = clean_and_transform(df)
        
        query = transformed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()
            
        query.awaitTermination()
    finally:
        spark_conn.stop()

if __name__ == "__main__":
    run()
