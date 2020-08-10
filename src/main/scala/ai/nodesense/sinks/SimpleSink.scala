package ai.nodesense.sinks

import ai.nodesense.models.DataValue
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}


class SimpleSink()
  extends RichSinkFunction[DataValue] {


  override def open(config: Configuration): Unit = {
    // open socket and writer

  }

  override def invoke(
                       value: DataValue,
                       ctx: SinkFunction.Context[_]): Unit = {
    println("SimpleSink Datavalue", value)
    // write sensor reading to socket
//    writer.println(value.toString)
//    writer.flush()
  }

  override def close(): Unit = {
    // close writer and socket
    //writer.close()
    //socket.close()
  }
}
