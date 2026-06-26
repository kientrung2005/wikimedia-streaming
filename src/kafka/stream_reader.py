import urllib.request
import ssl
import sys
import time

WIKIMEDIA_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'

def read_recent_changes():
    headers = {
        'Accept': 'text/event-stream',
        'User-Agent': 'Wikimedia-Streaming-App/1.0'
    }
    req = urllib.request.Request(WIKIMEDIA_STREAM_URL, headers=headers)
    ssl_context = ssl._create_unverified_context()
    
    retry_delay = 5
    while True:
        try:
            with urllib.request.urlopen(req, context=ssl_context) as response:
                for line in response:
                    decoded_line = line.decode('utf-8').strip()
                    if decoded_line.startswith('data:'):
                        data_str = decoded_line[5:].strip()
                        if data_str:
                            yield data_str
        except urllib.error.URLError as e:
            print(f"Lỗi kết nối SSE: {e.reason}. Đang thử lại sau {retry_delay} giây...", file=sys.stderr)
            time.sleep(retry_delay)
        except Exception as e:
            print(f"Lỗi không xác định trong stream: {e}. Đang thử lại sau {retry_delay} giây...", file=sys.stderr)
            time.sleep(retry_delay)
