package org.example.util

import java.util.Calendar

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

case class DataValue2(name: String, value: Double, timestamp: Long)

class DataValueTimeAssigner
  extends BoundedOutOfOrdernessTimestampExtractor[DataValue2](Time.seconds(5)) {

  /** Extracts timestamp from DataValue. */
  override def extractTimestamp(r: DataValue2): Long = r.timestamp
}

class TotalizerSource extends RichParallelSourceFunction[DataValue2] {

  // flag indicating whether source is still running.
  var running: Boolean = true

  /** run() continuously emits DataValue by emitting them through the SourceContext. */
  override def run(srcCtx: SourceContext[DataValue2]): Unit = {

    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

    var name = s"FLW0$taskIdx.FLOW_TOTAL"

    var total = 1000;
    val delta = 20;
    // emit data until being canceled
    while (running) {

      // update temperature
      total += delta

      // get current time
      val curTime = Calendar.getInstance.getTimeInMillis

      // emit new DataValue
      srcCtx.collect(DataValue2(name, total, curTime))

      // wait for 100 ms
      Thread.sleep(1 * 1000)
    }
  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }

}