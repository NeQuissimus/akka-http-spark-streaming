import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.{IncomingConnection, ServerBinding}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.unmarshalling.PredefinedFromEntityUnmarshallers._
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.spark.streaming.receiver.ActorHelper
import org.apache.spark._
import org.apache.spark.streaming._
import scala.concurrent.Future

class ForwarderActor extends Actor with ActorHelper {
  def receive = {
    case data: String => store(data)
  }
}

object Main extends App {
  implicit val system = ActorSystem("sparkDriverActorSystem")
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  val host = "localhost"
  val restPort = 9090
  val actorName = "forwarder"
  val sparkDriverPort = 7777

  // Spark Streaming
  val conf = new SparkConf().setMaster("local[*]").setAppName("TestWebApp").set("spark.driver.port", sparkDriverPort.toString).set("spark.driver.host", host).set("spark.akka.heartbeat.interval", "1s")
  val ssc = new StreamingContext(conf, Seconds(30))
  ssc.actorStream[String](Props[ForwarderActor], actorName).print()

  // Akka HTTP
  val restSource: Source[IncomingConnection, Future[ServerBinding]] = Http().bind(interface = host, port = restPort)

  val handler: HttpRequest => HttpResponse = {
    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<html><body>Hi there!</body></html>"))

    case HttpRequest(POST, Uri.Path("/data"), _, entity, _) if entity.contentType == ContentTypes.`application/json` => {
      val url: String = s"akka.tcp://sparkDriverActorSystem@$host:${sparkDriverPort + 1}/user/Supervisor0/$actorName"
      val s: Future[String] = Unmarshal(entity).to[String]
      s foreach (system.actorSelection(url) ! _)

      HttpResponse(200)
    }

    case _: HttpRequest =>
      HttpResponse(404, entity = "Page not found!")
  }

  // Start all the things
  ssc.start()

  val binding: Future[ServerBinding] = restSource.to(Sink.foreach { _ handleWithSyncHandler handler }).run()
}
