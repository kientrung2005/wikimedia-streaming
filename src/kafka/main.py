import json
import sys
import os
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)
sys.path.append(current_dir)

from stream_reader import read_recent_changes
from kafka_producer import WikimediaKafkaProducer
from config.kafka_config import get_kafka_config

def run():
    if sys.stdout.encoding != 'utf-8':
        try:
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
        except AttributeError:
            pass

    print("=== BẮT ĐẦU LUỒNG INGESTION WIKIMEDIA KAFKA ===")
    
    kafka_cfg = get_kafka_config()
    try:
        producer = WikimediaKafkaProducer(bootstrap_servers=kafka_cfg.bootstrap_servers, topic=kafka_cfg.topic)
    except Exception as e:
        print(f"Không thể khởi động Kafka Producer: {e}. Kết thúc ứng dụng.", file=sys.stderr)
        sys.exit(1)

    print(f"Đang kết nối SSE, đọc và đẩy dữ liệu thô vào Kafka topic: '{kafka_cfg.topic}'...")
    print("Nhấn Ctrl+C để dừng chương trình bất cứ lúc nào.\n")
    
    count = 0
    last_log_time = time.time()
    try:
        for raw_event in read_recent_changes():
            try:
                event_data = json.loads(raw_event)
                wiki = event_data.get('wiki', 'unknown')
                
                producer.send(key=wiki, value_str=raw_event)
                
                count += 1
                current_time = time.time()
                if current_time - last_log_time >= 5.0:
                    print(f"[INGESTION] Đã gửi tin nhắn thứ {count} vào Kafka (Wiki: {wiki})")
                    sys.stdout.flush()
                    last_log_time = current_time
            except json.JSONDecodeError:
                pass
            except Exception as e:
                print(f"Lỗi khi xử lý tin nhắn: {e}", file=sys.stderr)
    except KeyboardInterrupt:
        print("\n[INGESTION] Nhận tín hiệu dừng từ người dùng.")
    finally:
        producer.close()
        print(f"[INGESTION] Tổng số tin nhắn đã gửi thành công: {count}")
        print("=== ĐÃ DỪNG HỆ THỐNG INGESTION WIKIMEDIA KAFKA ===")

if __name__ == '__main__':
    run()
