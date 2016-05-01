import common.Config;
import common.PropertyFileParser;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cloudera.spark.streaming.kafka.JavaDStreamKafkaWriter;
import org.cloudera.spark.streaming.kafka.JavaDStreamKafkaWriterFactory;
import org.cloudera.spark.streaming.kafka.JavaRDDKafkaWriter;
import org.cloudera.spark.streaming.kafka.JavaRDDKafkaWriterFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class KafkaSparkProducer {

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

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> dataRDD = ctx.textFile(args[1]);

        JavaRDDKafkaWriter<String> writer = JavaRDDKafkaWriterFactory.fromJavaRDD(dataRDD);
        writer.writeToKafka(props, new KafkaSparkProc(
                propertyParser.getProperty(Config.KAFKA_TOPIC_KEY)));

        ctx.close();
    }
}


