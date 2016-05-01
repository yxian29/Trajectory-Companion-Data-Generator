import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


public class KafkaReadFileProc {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReadFileProc.class);

    private String kafkaHostname;
    private Integer kafkaPort;
    private String topic;
    private String inputFileName;
    private long messageRate;

    private KafkaReadFileProc(Builder builder) {
        this.kafkaHostname = builder.kafkaHostname;
        this.kafkaPort = builder.kafkaPort;
        this.topic = builder.topic;
        this.inputFileName = builder.inputFileName;
        this.messageRate = builder.messageRate;
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
        private long messageRate = 0l;

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

        public Builder setMessageRate(long messageRate) {
            this.messageRate = messageRate;
            return this;
        }

        public KafkaReadFileProc build() {
            KafkaReadFileProc producer = new KafkaReadFileProc(this);
            return producer;
        }
    }

    public void produceMessages() throws Exception{

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
                Thread.sleep(messageRate);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
