package org.rgcase.reactivekafka

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.server.Directives._
import akka.kafka.ProducerMessage.Message
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.concurrent.Future

class ReactiveKafkaFromHttpScala {

  implicit val system = ActorSystem("fromhttp")
  implicit val mat = ActorMaterializer()
  import system.dispatcher

  val serverSource = Http().bind(interface = "localhost", port = 8080)

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new ByteArraySerializer)
    .withBootstrapServers("localhost:9092")

  def toProducerMessage(topic: String)(chunk: Array[Byte]) = {
    Message[Array[Byte], Array[Byte], NotUsed](new ProducerRecord(topic, chunk), NotUsed)
  }

  val handler: HttpRequest ⇒ Future[HttpResponse] = {

    // Chunked payload
    case HttpRequest(POST, Uri.Path("/chunkedpayload"), _, entity, _) ⇒
      entity
        .dataBytes
        .map(chunk ⇒ toProducerMessage("targetchunktopic")(chunk.toArray))
        .via(Producer.flow(producerSettings))
        .runWith(Sink.ignore)
        .map(_ ⇒ HttpResponse(StatusCodes.Created))
        .recover {
          case e ⇒
            entity.discardBytes()
            HttpResponse(StatusCodes.InternalServerError)
        }

    // Full payload
    case HttpRequest(POST, Uri.Path("/fullpayload"), _, entity, _) ⇒
      val fut =
        entity
          .dataBytes
          .runWith(Sink.fold(Array.empty[Byte]) { case (acc, bytes) ⇒ acc ++ bytes })

      Source
        .fromFuture(fut)
        .map(bytes ⇒ toProducerMessage("targetfulltopic")(bytes))
        .via(Producer.flow(producerSettings))
        .runWith(Sink.ignore)
        .map(_ ⇒ HttpResponse(StatusCodes.Created))
        .recover {
          case e ⇒
            entity.discardBytes()
            HttpResponse(StatusCodes.InternalServerError)
        }

  }

  serverSource.to(Sink.foreach { connection ⇒
    connection.handleWithAsyncHandler(handler)
  }).run()

}