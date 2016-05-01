import common.Config;
import common.PropertyFileParser;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cloudera.spark.streaming.kafka.JavaDStreamKafkaWriter;
import org.cloudera.spark.streaming.kafka.JavaDStreamKafkaWriterFactory;

import java.util.Arrays;
import java.util.Properties;

public class KafkaSparkStreamingProducer {
    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.err.println("USAGE: <propsfile> <inputfile> [debug]");
            System.exit(1);
        }

        boolean isDebug = Arrays.asList(args).contains("debug");

        // Setup the property parser
        PropertyFileParser propertyParser = new PropertyFileParser(args[0]);
        propertyParser.parseFile();

        // Kafka producer
        Properties props = new Properties();
        props.put("metadata.broker.list",
                propertyParser.getProperty(Config.KAFKA_HOSTNAME_KEY)+":"+
                        propertyParser.getProperty(Config.KAFKA_PORT_KEY));
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        // Spark config
        SparkConf sparkConf = new SparkConf().
                setAppName("kafkaDataGenerator");
        if(isDebug) sparkConf.setMaster("local[*]");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        JavaDStream<String> fileStream = ssc.textFileStream(args[1]);

        JavaDStreamKafkaWriter<String> writer1 = JavaDStreamKafkaWriterFactory.fromJavaDStream(fileStream);
        writer1.writeToKafka(props, new KafkaSparkProc(
                propertyParser.getProperty(Config.KAFKA_TOPIC_KEY)));

        ssc.start();
        ssc.awaitTermination();
    }
}
