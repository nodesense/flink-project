package ai.nodesense.functions

import java.util.Date

import ai.nodesense.models.DataValue
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.example.Cumulative


class DataValueCumulativeProcessFunction
  extends ProcessWindowFunction[DataValue, Cumulative, String, TimeWindow] {

  private val previousFiringState = new ValueStateDescriptor[Long]("previous-firing", classOf[Long])

  // private val firingCounterState = new ReducingStateDescriptor[_]("firing-counter", new Sum, LongSerializer.INSTANCE)

  override def process(
                        key: String,
                        ctx: Context,
                        vals: Iterable[DataValue],
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

