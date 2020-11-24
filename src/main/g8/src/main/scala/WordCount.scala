import java.util.{ Locale, Properties }

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.String
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }
import zio._

/**
 * WordCount program
 * - reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * - split each text line into words
 * - compute the word occurence histogram
 *  -write the continuous updated histogram into a topic "streams-wordcount-output" where each record is an updated count of a single word.
 */
object WordCount extends App {

  def run(args: List[String]) =
    kafkaStreams.useForever.exitCode

  def kafkaStreams: ZManaged[Any, Throwable, KafkaStreams] =
    ZManaged.make(ZIO {

      val topology = createWordCountTopology

      val props = new Properties
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount")
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

      val ks = new KafkaStreams(topology, props)
      ks.start()
      ks
    })(ks => ZIO(ks.close()).orDie)

  def createWordCountTopology: Topology = {
    val builder = new StreamsBuilder
    builder
      .stream[String, String]("streams-plaintext-input")
      .flatMapValues((value: String) => value.toLowerCase(Locale.getDefault).split("\\W+"))
      .groupBy((_: String, value: String) => value)
      .count()(Materialized.as[String, Long, KeyValueStore[Bytes, Array[Byte]]]("counts-store"))
      .toStream
      .to("streams-wordcount-output")(Produced.`with`(Serdes.String, Serdes.Long))
    builder.build()
  }

}
