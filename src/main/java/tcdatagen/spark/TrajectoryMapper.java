package tcdatagen.spark;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class TrajectoryMapper implements
        Function<Tuple2<Integer, String>, String> {
    @Override
    public String call(Tuple2<Integer, String> input) throws Exception {
        return input._2();
    }
}
