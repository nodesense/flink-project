package org.example

import java.util.Date
import java.util.concurrent.TimeUnit

import ai.nodesense.analytics.{CumulativeAnalytic, StreamAnalytic}
import ai.nodesense.engine.AnalyticEngine
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



object Main extends  App {
  val engine = new AnalyticEngine()
  engine.run()
}
