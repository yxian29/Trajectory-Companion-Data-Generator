import common.Config;
import common.PropertyFileParser;
import kafka.producer.ProducerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Properties;

public class KafkaSparkProducer {

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.err.println("USAGE: <propsfile> <inputfile>");
            System.exit(1);
        }

        // Setup the property parser
        PropertyFileParser propertyParser = new PropertyFileParser(args[0]);
        propertyParser.parseFile();

        // Kafka producer
        Properties props = new Properties();
        props.put("metadata.broker.list",
                propertyParser.getProperty(Config.KAFKA_HOSTNAME_KEY)+":"+
                        propertyParser.getProperty(Config.KAFKA_PORT_KEY));
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);
        KafkaSink dataSink = new KafkaSink(config,
                propertyParser.getProperty(Config.KAFKA_TOPIC_KEY));

        // Spark config
        SparkConf sparkConf = new SparkConf().
                setAppName("kafkaSparkProducer");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> dataRDD = ctx.textFile(args[1]);

        final Broadcast<KafkaSink> broadcastKafkaSink = ctx.broadcast(dataSink);
        dataRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                broadcastKafkaSink.getValue().send(s);
            }
        });

        ctx.close();
    }
}
