/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package my.study.frauddetection;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

  private static final long serialVersionUID = 1L;
  private static final double SMALL_AMOUNT = 1.00;
  private static final double LARGE_AMOUNT = 500.00;
  private static final long DELAY_TS = 60 * 100;
  private static final Logger log = LoggerFactory.getLogger(FraudDetector.class);

  private transient ValueState<Boolean> flagState;
  private transient ValueState<Long> timerState;


  @Override
  public void open(Configuration parameters) {
    ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
        "flag",
        Types.BOOLEAN);
    flagState = getRuntimeContext().getState(flagDescriptor);

    ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
        "timer-state",
        Types.LONG);
    timerState = getRuntimeContext().getState(timerDescriptor);
  }


  @Override
  public void processElement(
      Transaction transaction,
      Context context,
      Collector<Alert> collector) throws Exception {
//    log.info("Transaction received: {}", transaction);
    Boolean prevLessThanOne = flagState.value();
    if (transaction.getAmount() < SMALL_AMOUNT) {
      flagState.update(true);

      // set the timer and timer state
      long timer = context.timerService().currentProcessingTime() + DELAY_TS;
      context.timerService().registerProcessingTimeTimer(timer);
      timerState.update(timer);
      log.info("timer set for key:{}", context.getCurrentKey());
    }

    if (prevLessThanOne != null) {
      if (prevLessThanOne && transaction.getAmount() > LARGE_AMOUNT) {
        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());
        log.info("new alert: {} for trx :{} and prev trx flag:{} has been created.", alert, transaction,
            prevLessThanOne);
        collector.collect(alert);
      }
      cleanUp(context);
    }
  }

  private void cleanUp(Context ctx) throws Exception {
    // delete timer
    Long timer = timerState.value();
    ctx.timerService().deleteProcessingTimeTimer(timer);

    // clean up all state
    timerState.clear();
    flagState.clear();
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
    log.info("timer cleanup called for key:{}", ctx.getCurrentKey());
    // remove flag after 1 minute
    timerState.clear();
    flagState.clear();
  }


}
