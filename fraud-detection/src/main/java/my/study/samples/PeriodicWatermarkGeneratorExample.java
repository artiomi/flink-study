package my.study.samples;

import my.study.samples.models.MyEvent;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class PeriodicWatermarkGeneratorExample {

  /**
   * This generator generates watermarks assuming that elements arrive out of order, but only to a certain degree. The
   * latest elements for a certain timestamp t will arrive at most n milliseconds after the earliest elements for
   * timestamp t.
   */
  public static class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<MyEvent> {

    private final long maxOutOfOrderness = 3500; // 3.5 seconds

    private long currentMaxTimestamp;

    @Override
    public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
      currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      // emit the watermark as current highest timestamp minus the out-of-orderness bound
      output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }

  }

  /**
   * This generator generates watermarks that are lagging behind processing time by a fixed amount. It assumes that
   * elements arrive in Flink after a bounded delay.
   */
  public static class TimeLagWatermarkGenerator implements WatermarkGenerator<MyEvent> {

    private final long maxTimeLag = 5000; // 5 seconds

    @Override
    public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
      // don't need to do anything because we work on processing time
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
    }
  }


}
