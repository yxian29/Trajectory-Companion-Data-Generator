package tcdatagen.common;

public class Config {
    // Props file
    public static final String DEFAULT_PROPS_FILE = "default.properties";

    // Zookeeper
    public static final String ZOOKEEPER_PORT_KEY = "zookeeper.port";
    public static final String ZOOKEEPER_HOST_KEY = "zookeeper.host";
    public static final String ZOOKEEPER_CONNECTION_STRING_KEY = "zookeeper.connection.string";

    // Zookeeper Test
    public static final String ZOOKEEPER_TEMP_DIR_KEY = "zookeeper.temp.dir";

    // Kafka
    public static final String KAFKA_HOSTNAME_KEY = "kafka.hostname";
    public static final String KAFKA_PORT_KEY = "kafka.port";
    public static final String KAFKA_TOPIC_KEY = "kafka.topic";
    public static final String KAFKA_PRDOUCER_MESSAGE_RATE = "kafka.producer.messagerate";

    // Kafka Test
    public static final String KAFKA_TEST_MESSAGE_COUNT_KEY = "kafka.test.message.count";
    public static final String KAFKA_TEST_BROKER_ID_KEY = "kafka.test.broker.id";
    public static final String KAFKA_TEST_TEMP_DIR_KEY = "kafka.test.temp.dir";
}
