package my.study.samples;

import my.study.samples.models.MyEvent;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class PunctuatedWatermarkGeneratorExample {

  public class PunctuatedAssigner implements WatermarkGenerator<MyEvent> {

    @Override
    public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
      if (event.hasWatermarkMarker()) {
        output.emitWatermark(new Watermark(event.getWatermarkTimestamp()));
      }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      // don't need to do anything because we emit in reaction to events above
    }
  }

}
