package tcdatagen.spark;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class TimestampFilter implements
        Function<Tuple2<Integer, String>, Boolean> {

    private int timestamp;

    public TimestampFilter(int timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public Boolean call(Tuple2<Integer, String> input) throws Exception {
        return input._1() == timestamp;
    }
}
