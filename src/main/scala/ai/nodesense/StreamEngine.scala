package ai.nodesense

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import java.util.Date
import java.util.concurrent.TimeUnit

import ai.nodesense.functions.{CumulativeTotalizerFunction, DataValueCumulativeProcessFunction}
import ai.nodesense.models.{DataValue, DataValueTimeAssigner}
import ai.nodesense.sinks.SimpleSink
import ai.nodesense.sources.TotalizerSource
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.commons.math3.stat.descriptive.summary.Sum
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.api.common.state.ReducingState
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.util.Collector
import org.example.{Cumulative, CumulativeProcessFunction}

/*
 DB config

  CumulativeAnalytic {
      input topic,
        IN: TOTAL_FLOW
        Output: CUMULATIVE_FLOW
      output topic
}
 */

class StreamEngine(val env: StreamExecutionEnvironment) {

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

   def run() = {

     // ingest sensor stream
     val dataValueStream: DataStream[DataValue] = getInputStream()


     val processStream: DataStream[DataValue] = dataValueStream
       .keyBy(_.name)
       //.timeWindow(Time.seconds(15))
       .process(new CumulativeTotalizerFunction)

     processStream.addSink(getOutputStream())

     processStream.print();

     val sink: StreamingFileSink[String] = StreamingFileSink
       .forRowFormat(new Path("/Users/krish/test"), new SimpleStringEncoder[String]("UTF-8"))
       .withRollingPolicy(
         DefaultRollingPolicy.builder()
           .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
           .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
           .withMaxPartSize(1024 * 1024 * 1024)
           .build())
       .build()


     val stringStream: DataStream[String] = processStream
         .map(  v => s"$v.name:$v.value:$v.timestamp")

     stringStream.addSink(sink)

   }
}
