package my.study.samples;

import java.time.Duration;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class IncrementalAgg {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSink<Tuple3<String, Long, String>> stream = env
        .fromElements("Apache", "DROP", "Flink", "IGNORE")
        .assignTimestampsAndWatermarks(watermarkStrategy())
        .keyBy(x -> x)
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .reduce(new MyReducingMax(), new MyWindowFunction())
        .print();

    env.execute();


  }

  private static WatermarkStrategy<String> watermarkStrategy() {

    return WatermarkStrategy
        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(20))
        .withTimestampAssigner((event, timestamp) -> Instant.now().toEpochMilli());

  }


  private static class MyReducingMax implements ReduceFunction<String> {

    public String reduce(String r1, String r2) {
      return r1.length() > r2.length() ? r1 : r2;
    }
  }

  private static class MyWindowFunction extends ProcessWindowFunction<
      String, Tuple3<String, Long, String>, String, TimeWindow> {

    @Override
    public void process(String key,
        ProcessWindowFunction<String, Tuple3<String, Long, String>, String, TimeWindow>.Context context,
        Iterable<String> elements,
        Collector<Tuple3<String, Long, String>> out) {
      String next = elements.iterator().next();
      out.collect(Tuple3.of(key, context.window().getEnd(), next));
    }

  }
}
