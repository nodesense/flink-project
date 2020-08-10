package ai.nodesense.engine

import ai.nodesense.analytics.{CumulativeAnalytic, StreamAnalytic}
import ai.nodesense.models.{Analytic, AnalyticContext, AnalyticDefinition}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.Date
import java.util.concurrent.TimeUnit

import ai.nodesense.analytics.{CumulativeAnalytic, StreamAnalytic}
import ai.nodesense.models.{Analytic, AnalyticContext, AnalyticDefinition}
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
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.util.Collector



class AnalyticEngine {

  def runAnalytic(context: AnalyticContext) = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    val streamAnalytic: StreamAnalytic = context.analytic.name match  {
      case "CumulativeAnalytic" => new CumulativeAnalytic(context, env)
      case _ => null
    }

    if (streamAnalytic != null) {
      streamAnalytic.run()
    }

    env.execute()
  }

  def run() = {
    val analytic = Analytic("1", "CumulativeAnalytic");
    val analyticDefinition = AnalyticDefinition("1", "FlowCum")

    val context = AnalyticContext(analytic, analyticDefinition)

    runAnalytic(context)
  }

}
