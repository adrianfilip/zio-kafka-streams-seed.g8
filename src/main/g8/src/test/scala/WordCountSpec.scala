import java.util.Properties

import org.apache.kafka.common.serialization.{ LongDeserializer, StringDeserializer, StringSerializer }
import org.apache.kafka.streams.{ StreamsConfig, TopologyTestDriver }
import zio.test.Assertion.equalTo
import zio.test.{ assert, suite, testM, DefaultRunnableSpec }
import zio.{ Managed, Task }

import scala.jdk.CollectionConverters.MapHasAsScala

object WordCountSpec extends DefaultRunnableSpec {
  def spec = suite("WordCountSpec")(
    testM("WordCount topology counts words occurances correctly at every step") {
      for {
        t <- testDriver.use(testDriver =>
              Task {
                val stringSerializer   = new StringSerializer()
                val stringDeserializer = new StringDeserializer()
                val longDeserializer   = new LongDeserializer()
                val inputTopic =
                  testDriver.createInputTopic("streams-plaintext-input", stringSerializer, stringSerializer)
                val outputTopic =
                  testDriver.createOutputTopic("streams-wordcount-output", stringDeserializer, longDeserializer)

                inputTopic.pipeInput("If you’re happy and you know it, clap your hands. (clap clap)")
                val a1 = assert(toScala(outputTopic.readKeyValuesToMap().asScala.toMap))(
                  equalTo(
                    Map[String, Long](
                      "re"    -> 1,
                      "hands" -> 1,
                      "and"   -> 1,
                      "clap"  -> 3,
                      "happy" -> 1,
                      "know"  -> 1,
                      "it"    -> 1,
                      "your"  -> 1,
                      "if"    -> 1,
                      "you"   -> 2
                    )
                  )
                )

                inputTopic.pipeInput("If you’re happy and you know it, clap your hands. (clap clap)")
                val a2 = assert(toScala(outputTopic.readKeyValuesToMap().asScala.toMap))(
                  equalTo(
                    Map[String, Long](
                      "re"    -> 2,
                      "hands" -> 2,
                      "and"   -> 2,
                      "clap"  -> 6,
                      "happy" -> 2,
                      "know"  -> 2,
                      "it"    -> 2,
                      "your"  -> 2,
                      "if"    -> 2,
                      "you"   -> 4
                    )
                  )
                )

                inputTopic.pipeInput("If you’re happy and you know it, and you really want to show it.")
                val a3 = assert(toScala(outputTopic.readKeyValuesToMap().asScala.toMap))(
                  equalTo(
                    Map[String, Long](
                      "re"     -> 3,
                      "and"    -> 4,
                      "happy"  -> 3,
                      "want"   -> 1,
                      "show"   -> 1,
                      "know"   -> 3,
                      "it"     -> 4,
                      "to"     -> 1,
                      "if"     -> 3,
                      "you"    -> 7,
                      "really" -> 1
                    )
                  )
                )

                inputTopic.pipeInput("If you’re happy and you know it, clap your hands.    (clap clap)")
                val a4 = assert(toScala(outputTopic.readKeyValuesToMap().asScala.toMap))(
                  equalTo(
                    Map[String, Long](
                      "re"    -> 4,
                      "hands" -> 3,
                      "and"   -> 5,
                      "clap"  -> 9,
                      "happy" -> 4,
                      "know"  -> 4,
                      "it"    -> 5,
                      "your"  -> 3,
                      "if"    -> 4,
                      "you"   -> 9
                    )
                  )
                )
                a1 && a2 && a3 && a4
              }
            )

      } yield t
    }
  )

  def toScala(map: Map[String, java.lang.Long]): Map[String, Long] = map.map(x => x._1 -> x._2.longValue())

  def testDriver: Managed[Throwable, TopologyTestDriver] =
    Managed.make(Task {
      // using DSL
      val topology = WordCount.createWordCountTopology

      // setup test driver
      val props = new Properties()
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
      new TopologyTestDriver(topology, props)
    })(testDriver => Task(testDriver.close()).orDie)
}
