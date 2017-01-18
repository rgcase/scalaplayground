package org.rgcase.reactivekafka

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{ CommittableMessage, CommittableOffsetBatch }
import akka.kafka.ProducerMessage.Message
import akka.kafka.scaladsl.{ Consumer, Producer }
import akka.kafka.{ ConsumerSettings, ProducerSettings, Subscriptions }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Sink }
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer }

class ReactiveKafkaSingleConsumerMultipleProducerScala extends App {

  implicit val system = ActorSystem("reactivekafkascala")
  implicit val mat = ActorMaterializer()

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9093")

  val kafkaSource =
    Consumer.committableSource(consumerSettings, Subscriptions.topics("sourcetopic"))

  def toProducerMessage(topic: String) = (msg: CommittableMessage[Array[Byte], String]) ⇒
    Message[Array[Byte], String, CommittableMessage[Array[Byte], String]](new ProducerRecord(topic, msg.record.value), msg)

  val producerFlow1 =
    Flow.fromFunction(toProducerMessage("targettopic1")).via(Producer.flow(producerSettings)).map(_.message.passThrough)

  val producerFlow2 =
    Flow.fromFunction(toProducerMessage("targettopic2")).via(Producer.flow(producerSettings)).map(_.message.passThrough)

  val producerFlow3 =
    Flow.fromFunction(toProducerMessage("targettopic3")).via(Producer.flow(producerSettings)).map(_.message.passThrough)

  kafkaSource
    .via(producerFlow1)
    .via(producerFlow2)
    .via(producerFlow3)
    .batch(max = 20, first ⇒ CommittableOffsetBatch.empty.updated(first.committableOffset)) { (batch, elem) ⇒
      batch.updated(elem.committableOffset)
    }.map(_.commitScaladsl())
    .runWith(Sink.ignore)

}