package my.study.samples;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class KeyProcessStream {

  private static final OutputTag<TaxiFare> LATE_FARES = new OutputTag<TaxiFare>("lateFares") {
  };

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<TaxiFare> fares = env.fromElements(new TaxiFare());

    SingleOutputStreamOperator<Tuple3<Long, Long, Float>> hourlyTips = fares
        .keyBy((TaxiFare fare) -> fare.driverId)
        .process(new PseudoWindow(Time.hours(1)));

    hourlyTips.getSideOutput(LATE_FARES).print();

    env.execute("Key process fares");
  }

  public static class PseudoWindow extends KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {


    private final long durationMsec;
    private transient MapState<Long, Float> sumOfTips;


    public PseudoWindow(Time duration) {
      this.durationMsec = duration.toMilliseconds();
    }

    @Override
    public void open(Configuration conf) {
      MapStateDescriptor<Long, Float> sumDesc = new MapStateDescriptor<>("sumOfTips", Long.class, Float.class);
      sumOfTips = getRuntimeContext().getMapState(sumDesc);

    }


    @Override
    public void processElement(TaxiFare fare,
        KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>>.Context ctx,
        Collector<Tuple3<Long, Long, Float>> out) throws Exception {

      long eventTime = fare.getEventTimeMillis();
      TimerService timerService = ctx.timerService();

      if (eventTime <= timerService.currentWatermark()) {
        ctx.output(LATE_FARES, fare);
      } else {
        // Round up eventTime to the end of the window containing this event.
        long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

        // Schedule a callback for when the window has been completed.
        timerService.registerEventTimeTimer(endOfWindow);

        // Add this fare's tip to the running total for that window.
        Float sum = sumOfTips.get(endOfWindow);
        if (sum == null) {
          sum = 0.0F;
        }
        sum += fare.tip;
        sumOfTips.put(endOfWindow, sum);
      }


    }

    @Override
    public void onTimer(long timestamp,
        KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>>.OnTimerContext ctx,
        Collector<Tuple3<Long, Long, Float>> out
    ) throws Exception {
      long driverId = ctx.getCurrentKey();
      // Look up the result for the hour that just ended.
      Float sumOfTips = this.sumOfTips.get(timestamp);

      Tuple3<Long, Long, Float> result = Tuple3.of(driverId, timestamp, sumOfTips);
      out.collect(result);
      this.sumOfTips.remove(timestamp);

    }
  }

}
