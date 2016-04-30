import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.Serializable;

public class KafkaSink implements Serializable{

    private Producer<String, String> producer;
    private String topic;

    public KafkaSink(ProducerConfig config, String topic) {
        this.producer = new Producer<>(config);
        this.topic = topic;
    }

    public void send(String value) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(this.topic, null, value);
        this.producer.send(data);
    }
}
