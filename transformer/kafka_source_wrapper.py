from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

class KafkaSourceWrapper:
    def __init__(self, topic: str, bootstrap_servers: str, group_id: str):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

    def build(self) -> KafkaSource:
        return KafkaSource.builder() \
            .set_bootstrap_servers(self.bootstrap_servers) \
            .set_topics(self.topic) \
            .set_group_id(self.group_id) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
