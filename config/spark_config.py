from typing import Optional, List, Dict
from pyspark.sql import SparkSession
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class SparkConnect:
    def __init__(
        self,
        app_name: str,
        master_url: Optional[str] = None,
        executor_memory: Optional[str] = None,
        executor_cores: Optional[int] = None,
        driver_memory: Optional[str] = None,
        num_executors: Optional[int] = None,
        jar_packages: Optional[List[str]] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        log_level: str = "WARN"
    ):
        self.app_name = app_name
        
        if master_url is None:
            master_url = os.getenv("SPARK_MASTER_URL", "local[*]")
        if executor_memory is None:
            executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
        if executor_cores is None:
            val = os.getenv("SPARK_EXECUTOR_CORES")
            executor_cores = int(val) if val else 2
        if driver_memory is None:
            driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "1g")
        if num_executors is None:
            val = os.getenv("SPARK_NUM_EXECUTORS")
            num_executors = int(val) if val else 1
        if jar_packages is None:
            jar_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]
            
        self.spark = self.create_spark_session(
            master_url,
            executor_memory,
            executor_cores,
            driver_memory,
            num_executors,
            jar_packages,
            spark_conf,
            log_level
        )

    def create_spark_session(
        self,
        master_url: str,
        executor_memory: Optional[str],
        executor_cores: Optional[int],
        driver_memory: Optional[str],
        num_executors: Optional[int],
        jar_packages: Optional[List[str]],
        spark_conf: Optional[Dict[str, str]],
        log_level: str
    ) -> SparkSession:
        builder = SparkSession.builder \
            .appName(self.app_name)
            
        if master_url:
            builder = builder.master(master_url)
        if executor_memory:
            builder = builder.config("spark.executor.memory", executor_memory)
        if executor_cores:
            builder = builder.config("spark.executor.cores", executor_cores)
        if driver_memory:
            builder = builder.config("spark.driver.memory", driver_memory)
        if num_executors:
            builder = builder.config("spark.executor.instances", num_executors)
        if jar_packages:
            jar_packages_url = ",".join(jar_packages)
            builder = builder.config("spark.jars.packages", jar_packages_url)
            
        builder = builder.config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")

        if spark_conf:
            for key, value in spark_conf.items():
                builder = builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)
        return spark

    def stop(self):
        if self.spark:
            self.spark.stop()
            print("-------Stop Spark Session--------")
