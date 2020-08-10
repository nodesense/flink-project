package ai.nodesense.models

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Assigns timestamps to SensorReadings based on their internal timestamp and
 * emits watermarks with five seconds slack.
 */
class DataValueTimeAssigner
  extends BoundedOutOfOrdernessTimestampExtractor[DataValue](Time.seconds(5)) {
  /** Extracts timestamp from SensorReading. */
  override def extractTimestamp(r: DataValue): Long = r.timestamp
}
