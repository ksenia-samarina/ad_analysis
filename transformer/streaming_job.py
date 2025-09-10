import json

from pyflink.common import Time, Types, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.common.watermark_strategy import WatermarkStrategy

from kafka_source_wrapper import KafkaSourceWrapper
from transformer.consts import SOURCE_KAFKA_TOPIC, SOURCE_KAFKA_BOOTSTRAP_SERVERS, EVENT_KEY_FIELD, \
    PROMETHEUS_METRICS_SERVER_PORT
from transformer.prometheus_server import start_prometheus_server
from transformer.transformations.transformation import Transformation


class StreamingJob:
    def __init__(self, source, env):
        self.source = source
        self.env = env

    def process(self, key_func, window, transformation):
        ds = self.env.from_source(
            source=self.source.build(),
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="Kafka Source"
        )

        ds.key_by(key_func, key_type=Types.INT()) \
          .window(window) \
          .process(transformation, output_type=Types.TUPLE([Types.INT(), Types.DOUBLE()]))

        self.env.execute("All metrics streaming job")


if __name__ == "__main__":
    start_prometheus_server(PROMETHEUS_METRICS_SERVER_PORT)

    # Конфигурация Flink
    conf = Configuration()
    conf.set_string(
        "pipeline.jars",
        "file:///home/ksenia/PycharmProjects/ad_analysis/transformer/jars/flink-connector-kafka-4.0.1-2.0.jar;"
        "file:///home/ksenia/PycharmProjects/ad_analysis/transformer/jars/kafka-clients-3.1.0.jar"
    )

    # Создание окружения (запуск JVM с jar)
    env = StreamExecutionEnvironment.get_execution_environment(conf)

    # Настраиваем Kafka source
    kafka_source = KafkaSourceWrapper(
        topic=SOURCE_KAFKA_TOPIC,
        bootstrap_servers=SOURCE_KAFKA_BOOTSTRAP_SERVERS,
        group_id="flink"
    )

    # Запуск джобы
    job = StreamingJob(kafka_source, env)

    job.process(
        key_func=lambda event: json.loads(event).get(EVENT_KEY_FIELD),
        window=SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)),
        transformation=Transformation(),
    )
