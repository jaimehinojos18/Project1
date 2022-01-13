package API
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import akka.http.scaladsl.unmarshalling.Unmarshal
import scala.concurrent.Future
import scala.util.{Failure, Success}
import java.io._
import scala.collection.mutable.ListBuffer



object API {
  def get(u:String, fileName:String): Unit ={
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    import system.dispatcher
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = u,
      headers = List(RawHeader("X-Rapidapi-Key","35825413cdmsh0abddeecc9829e2p113091jsncc2f74abccf8"))

    )
    val responseFuture = Http().singleRequest(request)
    val entityFut: Future[String] = {
      responseFuture.flatMap(resp => Unmarshal(resp.entity).to[String])
    }




    entityFut.onComplete {
      case Success(body) => {
        val pw = new PrintWriter(new File(s"C:/Users/ghost/IdeaProjects/Premier_League_Tracker/src/main/Files/$fileName.json" ))
        pw.write(body.toString)
        pw.close()

      }

      case Failure(_) =>
        println("failure")
    }




  }
  def getPlayers(id:Int): Unit ={

    var va = 0
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    import system.dispatcher
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = s"https://api-football-v1.p.rapidapi.com/v3/players?team=$id&season=2021",
      headers = List(RawHeader("X-Rapidapi-Key","35825413cdmsh0abddeecc9829e2p113091jsncc2f74abccf8"))

    )
    val responseFuture = Http().singleRequest(request)
    val entityFut: Future[String] = {
      responseFuture.flatMap(resp => Unmarshal(resp.entity).to[String])
    }

    entityFut.onComplete {
      case Success(body) => {
        val pw = new PrintWriter(new File(s"C:/Users/ghost/IdeaProjects/Premier_League_Tracker/src/main/Files/Players$id.csv" ))
        val json_data = ujson.read(body.toString)
        val players = json_data("response")
        val len = players.arr.size

        println(json_data)
        for(i <- 0 to len-1){
          var s = s"${players(i)("player")("id")},${players(i)("player")("name")}," +
            s"${players(i)("player")("nationality")},${players(i)("statistics")(0)("games")("appearences")}," +
            s"${players(i)("statistics")(0)("games")("minutes")},${players(i)("statistics")(0)("games")("position")}," +
            s"${players(i)("statistics")(0)("shots")("total")},${players(i)("statistics")(0)("goals")("total")}," +
            s"${players(i)("statistics")(0)("goals")("conceded")},${players(i)("statistics")(0)("goals")("assists")}," +
            s"${players(i)("statistics")(0)("passes")("total")},${players(i)("statistics")(0)("passes")("key")}," +
            s"${players(i)("statistics")(0)("passes")("accuracy")},${players(i)("statistics")(0)("cards")("yellow")}," +
            s"${players(i)("statistics")(0)("cards")("red")},$id\n"
          s = s.replaceAll("null", "0")
          pw.write(s)
        }
        pw.close()


      }

      case Failure(_) =>
        println("failure")
    }


  }



}
