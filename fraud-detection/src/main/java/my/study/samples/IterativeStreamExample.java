package my.study.samples;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterativeStreamExample {

  public static void main(String[] args) throws Exception {
    try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

      DataStream<Long> someIntegers = env.fromSequence(0, 5);

      IterativeStream<Long> iteration = someIntegers.iterate();

      DataStream<Long> minusOne = iteration.map((MapFunction<Long, Long>) value -> value - 1);

      DataStream<Long> stillGreaterThanZero = minusOne.filter((FilterFunction<Long>) value -> (value > 0));

      iteration.closeWith(stillGreaterThanZero);
      stillGreaterThanZero.print("Greater than 0 - ");
      DataStream<Long> lessThanZero = minusOne.filter((FilterFunction<Long>) value -> (value <= 0));
      lessThanZero.print("Less than 0 - ");

      JobExecutionResult execResult = env.execute();
      System.out.println("Execution result: " + execResult);
    }
  }
}
