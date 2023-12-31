package my.study.samples;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class TaxiFare implements Serializable {

  public long rideId;
  public long taxiId;
  public long driverId;
  public Instant startTime;
  public String paymentType;
  public float tip;
  public float tolls;
  public float totalFare;
  /**
   * Creates a TaxiFare with now as the start time.
   */
  public TaxiFare() {
    this.startTime = Instant.now();
  }
  /**
   * Creates a TaxiFare with the given parameters.
   */
  public TaxiFare(
      long rideId,
      long taxiId,
      long driverId,
      Instant startTime,
      String paymentType,
      float tip,
      float tolls,
      float totalFare) {
    this.rideId = rideId;
    this.taxiId = taxiId;
    this.driverId = driverId;
    this.startTime = startTime;
    this.paymentType = paymentType;
    this.tip = tip;
    this.tolls = tolls;
    this.totalFare = totalFare;
  }

  @Override
  public String toString() {

    return rideId
        + ","
        + taxiId
        + ","
        + driverId
        + ","
        + startTime.toString()
        + ","
        + paymentType
        + ","
        + tip
        + ","
        + tolls
        + ","
        + totalFare;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaxiFare taxiFare = (TaxiFare) o;
    return rideId == taxiFare.rideId
        && taxiId == taxiFare.taxiId
        && driverId == taxiFare.driverId
        && Float.compare(taxiFare.tip, tip) == 0
        && Float.compare(taxiFare.tolls, tolls) == 0
        && Float.compare(taxiFare.totalFare, totalFare) == 0
        && Objects.equals(startTime, taxiFare.startTime)
        && Objects.equals(paymentType, taxiFare.paymentType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        rideId, taxiId, driverId, startTime, paymentType, tip, tolls, totalFare);
  }

  /**
   * Gets the fare's start time.
   */
  public long getEventTimeMillis() {
    return startTime.toEpochMilli();
  }

  /**
   * Creates a StreamRecord, using the fare and its timestamp. Used in tests.
   */
  @VisibleForTesting
  public StreamRecord<TaxiFare> asStreamRecord() {
    return new StreamRecord<>(this, this.getEventTimeMillis());
  }
}
