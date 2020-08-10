package ai.nodesense.functions

import ai.nodesense.models.DataValue
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
//
//val Totalflow: DataStream[(String, Long, Double)] = keyedSensorData
//.process(new SelfCleaningTemperatureAlertFunction())


class CumulativeTotalizerFunction()
  extends KeyedProcessFunction[String, DataValue, DataValue] {


  // the state handle object
  private var lastKnownCumulative: ValueState[Double] = _
  private var lastTimerState: ValueState[Long] = _
  private var lastKnownValue: ValueState[Double] = _


  override def processElement(
                               reading: DataValue,
                               ctx: KeyedProcessFunction[String, DataValue, DataValue]#Context,
                               out: Collector[DataValue]): Unit = {


    lastTimerState.update(reading.timestamp)
    // fetch the last temperature from state
    var lastKnownCumulativeTemp = lastKnownCumulative.value()
    println("lastKnownCumulativeTemp-->>>>>>>>>>>"+lastKnownCumulativeTemp)
    if (lastKnownCumulativeTemp==null){
      lastKnownCumulativeTemp = 0;
    }
    var lastKnownValueTemp = lastKnownValue.value()
    if (lastKnownValueTemp==null){
      lastKnownValueTemp = 0;
    }

    // check if we need to emit an alert
    var deltaDiff = reading.value - lastKnownValueTemp
    //    print("value::"+reading.value)
    if(deltaDiff<0){
      lastKnownCumulativeTemp = lastKnownCumulativeTemp+reading.value
    }

    lastKnownCumulativeTemp =lastKnownCumulativeTemp+deltaDiff
    //    print("\tOld Cumulative Value::"+reading.value)



    //    if (tempDiff > threshold) {
    // temperature increased by more than the thresholdTimer

    //    out.collect((reading.name.split('.')(0)+"."+"CUMULATIVE_FLOW", reading.timestamp,lastKnownCumulativeTemp))

    val dataValueOut = DataValue(reading.device_id, reading.name, reading.tag, lastKnownCumulativeTemp, reading.timestamp)

    out.collect(dataValueOut)
    //    }

    // update lastTemp state
    this.lastKnownCumulative.update(lastKnownCumulativeTemp)
    this.lastKnownValue.update(reading.value)
  }

  override def open(parameters: Configuration): Unit = {

    // register state for last temperature
    val lastKnownCumulativeDescriptor = new ValueStateDescriptor[Double]("lastKnownCumulativeDescriptor", classOf[Double])
    lastKnownCumulative = getRuntimeContext.getState[Double](lastKnownCumulativeDescriptor)
    //register last know value
    val lastKnownValueDescriptor: ValueStateDescriptor[Double] =
      new ValueStateDescriptor[Double]("lastKnownValueDescriptor", classOf[Double])
    lastKnownValue = getRuntimeContext.getState(lastKnownValueDescriptor)
    // register state for last timer
    val timestampDescriptor: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("timestampState", classOf[Long])
    lastTimerState = getRuntimeContext.getState(timestampDescriptor)
    //    lastKnownCumulative.update(10000.0)
    println("lastKnownCumulative:: "+lastKnownCumulative)
  }

}

