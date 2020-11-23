import java.util.Properties

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.String
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import zio._

/**
 * LineSplit program
 * - reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * - writes the messages as-is into a sink topic "streams-pipe-output".
 */
object Pipe extends App {

  def run(args: List[String]) =
    prog.exitCode

  def prog =
    kafkaStreams().use { ks =>
      ZIO(ks.start()) *> ZIO.never
    }

  def kafkaStreams(): ZManaged[Any, Throwable, KafkaStreams] =
    ZManaged.make(ZIO {
      val builder = new StreamsBuilder

      val props = new Properties
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe")
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

      builder.stream("streams-plaintext-input").to("streams-pipe-output")

      val topology = builder.build()
      new KafkaStreams(topology, props)
    })(ks => ZIO(ks.close()).orDie)

}
