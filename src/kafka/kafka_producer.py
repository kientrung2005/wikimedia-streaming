import sys
import socket
from confluent_kafka import Producer

class WikimediaKafkaProducer:
    def __init__(self, bootstrap_servers='localhost:9094', topic='wikimedia-recentchanges'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': socket.gethostname(),
            'linger.ms': 100,
            'compression.type': 'gzip',
            'acks': 1
        }
        
        try:
            self.producer = Producer(conf)
            print(f"[KAFKA] Khởi tạo Producer thành công kết nối tới: {self.bootstrap_servers}")
        except Exception as e:
            print(f"[KAFKA] Lỗi khởi tạo Kafka Producer: {e}", file=sys.stderr)
            raise e

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"[-] Gửi tin nhắn đến Kafka thất bại: {err}", file=sys.stderr)

    def send(self, key, value_str):
        try:
            self.producer.produce(
                self.topic,
                key=key.encode('utf-8') if key else None,
                value=value_str.encode('utf-8'),
                callback=self._delivery_report
            )
            self.producer.poll(0)
        except BufferError:
            print("[KAFKA] Hàng đợi Producer bị đầy, đang tiến hành flush dữ liệu...", file=sys.stderr)
            self.producer.flush(1.0)
        except Exception as e:
            print(f"[-] Lỗi phát sinh khi gửi tin nhắn tới Kafka: {e}", file=sys.stderr)

    def close(self, timeout=3.0):
        print("[KAFKA] Đang flush để gửi nốt các tin nhắn còn tồn đọng trong bộ đệm...")
        self.producer.flush(timeout)
