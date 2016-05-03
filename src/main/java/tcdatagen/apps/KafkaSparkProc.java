package tcdatagen.apps;

import kafka.producer.KeyedMessage;
import org.apache.spark.api.java.function.Function;

public class KafkaSparkProc implements Function<String, KeyedMessage<String, String>> {

    private String topic;

    public KafkaSparkProc(String topic)
    {
        this.topic = topic;
    }

    public KeyedMessage<String, String> call(String s) throws Exception {
        return new KeyedMessage<>(topic, null, s);
    }
}