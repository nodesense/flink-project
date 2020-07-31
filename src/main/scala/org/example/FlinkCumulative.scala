package org.example

import org.example.util.{DataValue, DataValueTimeAssigner, SensorReading, SensorSource, SensorTimeAssigner, TotalizerSource}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkCumulative {

  def threshold = 25.0

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val sensorData: DataStream[DataValue] = env
      // SensorSource generates random temperature readings
      .addSource(new TotalizerSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new DataValueTimeAssigner)

    val cumulativePerWindow: DataStream[Cumulative] = sensorData
      .keyBy(_.name)
      .timeWindow(Time.seconds(5))
      .process(new CumulativeProcessFunction)


    cumulativePerWindow.print();

    env.execute()
  }
}


case class Cumulative(id: String, value:Double, timestamp: Long)

/**
 * A ProcessWindowFunction that computes the lowest and highest temperature
 * reading per window and emits a them together with the
 * end timestamp of the window.
 */
class CumulativeProcessFunction
  extends ProcessWindowFunction[DataValue, Cumulative, String, TimeWindow] {

  override def process(
                        key: String,
                        ctx: Context,
                        vals: Iterable[DataValue],
                        out: Collector[Cumulative]): Unit = {

    val values = vals.map(_.value)
    val windowEnd = ctx.window.getEnd

    out.collect(Cumulative("CUMULATIVE", values.min, windowEnd))
  }
}
