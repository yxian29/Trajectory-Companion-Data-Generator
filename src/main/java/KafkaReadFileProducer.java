import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


public class KafkaReadFileProducer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReadFileProducer.class);

    private String kafkaHostname;
    private Integer kafkaPort;
    private String topic;
    private String inputFileName;

    private KafkaReadFileProducer(Builder builder) {
        this.kafkaHostname = builder.kafkaHostname;
        this.kafkaPort = builder.kafkaPort;
    }

    public String getKafkaHostname() {
        return kafkaHostname;
    }

    public Integer getKafkaPort() {
        return kafkaPort;
    }

    public String getTopic() {
        return topic;
    }

    public String getInputFileName() {
        return inputFileName;
    }

    public static class Builder {
        private String kafkaHostname;
        private Integer kafkaPort;
        private String topic;
        private String inputFileName;

        public Builder setKafkaHostname(String kafkaHostname) {
            this.kafkaHostname = kafkaHostname;
            return this;
        }

        public Builder setKafkaPort(Integer kafkaPort) {
            this.kafkaPort = kafkaPort;
            return this;
        }

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder setInputFileName(String inputFileName) {
            this.inputFileName = inputFileName;
            return this;
        }

        public KafkaReadFileProducer build() {
            KafkaReadFileProducer producer = new KafkaReadFileProducer(this);
            return producer;
        }
    }

    public void produceMessages() {

        // Setup the Kafka Producer
        Properties props = new Properties();
        props.put("metadata.broker.list", getKafkaHostname() + ":" + getKafkaPort());
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // Read each line from the file and send via the producer
        try{
            BufferedReader br = new BufferedReader(new FileReader(getInputFileName()));
            String line = br.readLine();
            while (line != null) {
                KeyedMessage<String, String> data = new KeyedMessage<String, String>(getTopic(), null, line);
                producer.send(data);
                System.out.println(line);
                //Thread.sleep(200l);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
