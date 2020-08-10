package ai.nodesense.analytics

import java.util.concurrent.TimeUnit

import ai.nodesense.functions.CumulativeTotalizerFunction
import ai.nodesense.models.{AnalyticContext, DataValue, DataValueTimeAssigner}
import ai.nodesense.sinks.SimpleSink
import ai.nodesense.sources.TotalizerSource
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
 DB config

  CumulativeAnalytic {
      input topic,
        IN: TOTAL_FLOW
        Output: CUMULATIVE_FLOW
      output topic
}
 */

abstract class StreamAnalytic(val context: AnalyticContext, val env: StreamExecutionEnvironment) {

  def run() = {}

  def getInputStream(): DataStream[DataValue] = {
    val inputStream: DataStream[DataValue] = env
      // SensorSource generates random temperature readings
      .addSource(new TotalizerSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new DataValueTimeAssigner)
    inputStream
  }

  def getOutputStream(): RichSinkFunction[DataValue] = {
    new SimpleSink()
  }


}
