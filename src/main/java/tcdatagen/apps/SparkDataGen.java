package tcdatagen.apps;

import org.apache.spark.api.java.JavaPairRDD;
import tcdatagen.common.Config;
import tcdatagen.common.PropertyFileParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.cloudera.spark.streaming.kafka.JavaRDDKafkaWriter;
import org.cloudera.spark.streaming.kafka.JavaRDDKafkaWriterFactory;
import tcdatagen.spark.TimestampFilter;
import tcdatagen.spark.TimestampMapper;
import tcdatagen.spark.TrajectoryMapper;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class SparkDataGen {

    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.err.println("USAGE: <propsfile> <inputfile> [debug]");
            System.exit(1);
        }

        boolean isDebug = Arrays.asList(args).contains("debug");

        // Setup the property parser
        PropertyFileParser propertyParser = new PropertyFileParser(args[0]);
        propertyParser.parseFile();

        int messageRate = Integer.parseInt(propertyParser.getProperty
                (Config.KAFKA_PRDOUCER_MESSAGE_RATE));

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
        JavaRDD<String> lines = ctx.textFile(args[1]);

        // timestamp in ascending order
        JavaPairRDD<Integer, String> orderedDataRDD =
        lines.mapToPair(new TimestampMapper())
                .sortByKey().cache();

        // get all timestamps
        JavaRDD<Integer> timestampRDD = orderedDataRDD.keys().distinct();
        Set<Integer> timestamps = new TreeSet(timestampRDD.toArray());

        // iterate each timestamp
        for (int timestamp: timestamps) {

            // grab all trajectories per timestamp
            JavaRDD<String> timeDataRDD =
            orderedDataRDD.filter(new TimestampFilter(timestamp))
                    .map(new TrajectoryMapper());

            try{
                JavaRDDKafkaWriter<String> writer = JavaRDDKafkaWriterFactory.fromJavaRDD(timeDataRDD);
                writer.writeToKafka(props, new KafkaSparkProc(
                        propertyParser.getProperty(Config.KAFKA_TOPIC_KEY)));
                Thread.sleep(messageRate);
            }
            catch(Exception e)
            {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }

        ctx.close();
    }
}


