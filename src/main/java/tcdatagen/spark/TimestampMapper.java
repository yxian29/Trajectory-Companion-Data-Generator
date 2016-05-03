package tcdatagen.spark;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class TimestampMapper implements PairFunction<String, Integer, String> {
    @Override
    public Tuple2<Integer, String> call(String s) throws Exception {
        String[] split = s.split(",");
        Integer timestamp = 0;
        if(split[3].contains(":")) { // assuming HH:mm format
            timestamp = toSeconds(split[3]);
        }
        else { // assuming integer format
            timestamp = Integer.parseInt(split[3]);
        }

        return new Tuple2<>(timestamp, s);
    }

    private int toSeconds(String timestamp) {
        if(!timestamp.contains(":"))
            return 0;

        String[] split = timestamp.split(":");
        int sec = Integer.parseInt(split[0]) * 60 + Integer.parseInt(split[1]);
        return sec;
    }
}
