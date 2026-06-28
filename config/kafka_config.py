from dataclasses import dataclass
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic: str

def get_kafka_config() -> KafkaConfig:
    in_docker = os.path.exists('/.dockerenv')
    
    if in_docker:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS_SPARK", "kafka:9092")
    else:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:9094")
        
    explicit_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if explicit_servers:
        bootstrap_servers = explicit_servers

    return KafkaConfig(
        bootstrap_servers=bootstrap_servers,
        topic=os.getenv("KAFKA_TOPIC", "wikimedia-recentchanges")
    )
