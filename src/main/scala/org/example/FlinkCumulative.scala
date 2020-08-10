package org.example

import java.util.Date

import org.example.util.{DataValue2, DataValueTimeAssigner, SensorReading, SensorSource, SensorTimeAssigner, TotalizerSource}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.commons.math3.stat.descriptive.summary.Sum
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.api.common.state.ReducingState
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
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
    val sensorData: DataStream[DataValue2] = env
      // SensorSource generates random temperature readings
      .addSource(new TotalizerSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new DataValueTimeAssigner)

    val cumulativePerWindow: DataStream[Cumulative] = sensorData
      .keyBy(_.name)
      .timeWindow(Time.seconds(60))
      .process(new CumulativeProcessFunction)
      //.map(v => (v.name, Cumulative(v.name, v.value, v.timestamp)))
      //.keyBy(_.name)
      //.process(new CountWithTimeoutFunction())

    cumulativePerWindow.print();

    env.execute()
  }
}


case class Cumulative(name: String, value:Double, timestamp: Long)
case class CumulativeOut(name: String, value:Double, timestamp: Long)

/**
 * A ProcessWindowFunction that computes the lowest and highest temperature
 * reading per window and emits a them together with the
 * end timestamp of the window.
 */
class CumulativeProcessFunction
  extends ProcessWindowFunction[DataValue2, Cumulative, String, TimeWindow] {

  private val previousFiringState = new ValueStateDescriptor[Long]("previous-firing", classOf[Long])

  // private val firingCounterState = new ReducingStateDescriptor[_]("firing-counter", new Sum, LongSerializer.INSTANCE)

  override def process(
                        key: String,
                        ctx: Context,
                        vals: Iterable[DataValue2],
                        out: Collector[Cumulative]): Unit = {

    val previousFiring =   ctx.windowState.getState(previousFiringState)

    val value = previousFiring.value();

    vals.foreach(println(_))

    println("Value size ", vals.size)
    println("Prev Value is ", value)

    val values = vals.map(_.value)
    val windowEnd = ctx.window.getEnd
    println("Currnt value is ", values.min)
    println("------------")
    previousFiring.update(values.min.toLong)

    val d = new Date(windowEnd)
    println("datetime now ", d)
    out.collect(Cumulative("CUMULATIVE", values.min, windowEnd))
  }

  }

case class CountWithTimestamp(key: String, count: Double, lastModified: Long)


class CountWithTimeoutFunction extends KeyedProcessFunction[Tuple, Cumulative, CumulativeOut] {

  /** The state that is maintained by this process function */
  lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext
    .getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))


  override def processElement(
                               value: Cumulative,
                               ctx: KeyedProcessFunction[Tuple, Cumulative, CumulativeOut]#Context,
                               out: Collector[CumulativeOut]): Unit = {

    // initialize or retrieve/update the state
    val current: CountWithTimestamp = state.value match {
      case null =>
        CountWithTimestamp(value.name, value.value, ctx.timestamp)
      case CountWithTimestamp(key, count, lastModified) =>
        CountWithTimestamp(key, count + 1, ctx.timestamp)
    }

    // write the state back
    state.update(current)

    // schedule the next timer 60 seconds from the current event time
    ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[Tuple, Cumulative, CumulativeOut]#OnTimerContext,
                        out: Collector[CumulativeOut]): Unit = {

    state.value match {
      case CountWithTimestamp(key, count, lastModified) if (timestamp == lastModified + 60000) =>
        out.collect(CumulativeOut(key, count, System.currentTimeMillis()))
      case _ =>
    }
  }
}