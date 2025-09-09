from prometheus_client import start_http_server
import threading

from transformer.consts import PROMETHEUS_METRICS_SERVER_PORT

def start_prometheus_server(port=PROMETHEUS_METRICS_SERVER_PORT):
    threading.Thread(target=start_http_server, args=(port,), daemon=True).start()


if __name__ == '__main__':
    start_prometheus_server(port=PROMETHEUS_METRICS_SERVER_PORT)
