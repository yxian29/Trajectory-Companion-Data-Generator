import common.Config;
import common.PropertyFileParser;

public class KafkaReadFileProducerCli {

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.err.println("USAGE: <propsfile> <inputfile>");
            System.exit(1);
        }

        // Setup the property parser
        PropertyFileParser propertyParser = new PropertyFileParser(args[0]);
        propertyParser.parseFile();

        // Producer
        KafkaReadFileProducer kafkaReadfileProducer = new KafkaReadFileProducer.Builder()
                .setKafkaHostname(propertyParser.getProperty(Config.KAFKA_HOSTNAME_KEY))
                .setKafkaPort(Integer.parseInt(propertyParser.getProperty(Config.KAFKA_PORT_KEY)))
                .setTopic(propertyParser.getProperty(Config.KAFKA_TOPIC_KEY))
                .setMessageRate(Long.parseLong(propertyParser.getProperty(Config.KAFKA_PRDOUCER_MESSAGE_RATE)))
                .setInputFileName(args[1])
                .build();

        kafkaReadfileProducer.produceMessages();

    }
}
