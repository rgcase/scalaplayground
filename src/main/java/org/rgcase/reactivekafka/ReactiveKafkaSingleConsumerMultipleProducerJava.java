package org.rgcase.reactivekafka;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.*;
import akka.kafka.ConsumerMessage.CommittableMessage;
import akka.kafka.ConsumerMessage.CommittableOffsetBatch;
import akka.kafka.ProducerMessage.Message;
import akka.kafka.javadsl.*;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ReactiveKafkaSingleConsumerMultipleProducerJava {

    private static Message<byte[], String, CommittableMessage<byte[], String>> toProducerMessage(String topic, CommittableMessage<byte[], String> msg) {
        return new Message<>(new ProducerRecord<>(topic, msg.record().value()), msg);
    }

    public static void main(String[] args) {

        final ActorSystem system = ActorSystem.create("reactivekafka");
        final ActorMaterializer mat = ActorMaterializer.create(system);

        ConsumerSettings<byte[], String> consumerSettings =
            ConsumerSettings.create(system, new ByteArrayDeserializer(), new StringDeserializer())
                .withBootstrapServers("localhost:9092")
                .withGroupId("group1")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ProducerSettings<byte[], String> producerSettings =
            ProducerSettings.create(system, new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers("localhost:9092");

        Source<CommittableMessage<byte[], String>, Consumer.Control> kafkaSource =
            Consumer.committableSource(consumerSettings, Subscriptions.topics("yourtopic"));



        Flow<CommittableMessage<byte[], String>, CommittableMessage<byte[], String>, NotUsed> producerFlow1 =
            Flow
                .fromFunction((CommittableMessage<byte[], String> msg) -> toProducerMessage("targettopic1", msg))
                .via(Producer.flow(producerSettings))
                .map(msg -> msg.message().passThrough());

        Flow<CommittableMessage<byte[], String>, CommittableMessage<byte[], String>, NotUsed> producerFlow2 =
            Flow
                .fromFunction((CommittableMessage<byte[], String> msg) -> toProducerMessage("targettopic2", msg))
                .via(Producer.flow(producerSettings))
                .map(msg -> msg.message().passThrough());

        Flow<CommittableMessage<byte[], String>, CommittableMessage<byte[], String>, NotUsed> producerFlow3 =
            Flow
                .fromFunction((CommittableMessage<byte[], String> msg) -> toProducerMessage("targettopic3", msg))
                .via(Producer.flow(producerSettings))
                .map(msg -> msg.message().passThrough());

        kafkaSource
            .via(producerFlow1)
            .via(producerFlow2)
            .via(producerFlow3)
            .batch(
                20,
                first -> ConsumerMessage.emptyCommittableOffsetBatch().updated(first.committableOffset()),
                (batch, elem) -> batch.updated(elem.committableOffset()))
            .mapAsync(3, CommittableOffsetBatch::commitJavadsl)
            .runWith(Sink.ignore(), mat);

    }


}
