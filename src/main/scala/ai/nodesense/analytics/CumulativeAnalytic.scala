package ai.nodesense.analytics

import java.util.concurrent.TimeUnit

import ai.nodesense.functions.CumulativeTotalizerFunction
import ai.nodesense.models.{AnalyticContext, DataValue, DataValueTimeAssigner}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import java.util.concurrent.TimeUnit

import ai.nodesense.functions.CumulativeTotalizerFunction
import ai.nodesense.sinks.SimpleSink
import ai.nodesense.sources.TotalizerSource
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

class CumulativeAnalytic(context: AnalyticContext, env: StreamExecutionEnvironment) extends  StreamAnalytic (context, env) {
  override  def run() = {
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
