package Database
import org.apache.spark.sql.SparkSession
import User.Customer
import API.API
import org.apache.log4j.{Level, Logger}

import scala.util.Random
import java.io.FileNotFoundException
object spark_connect {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "C:/hadoop")
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  spark.sparkContext.setLogLevel("ERROR")
  def initiate(): Unit = {
    //val api = API
    //api.get("https://api-football-v1.p.rapidapi.com/v3/standings?season=2021&league=39", "PLTable")
    //insert_player()

    spark.sql("create table IF NOT EXISTS users (id int, username string, password string, firstName string, lastName string, team_id int, type string) row format delimited fields terminated by ','")
    spark.sql("drop table if exists PL_Table")
    spark.sql("create table IF NOT EXISTS PL_Table(id Int, rank int,team String, played int, win int, draw int, loss int, goalsFor int, goalsAgainst int, home_played int, home_win int, home_draw int, " +
      "home_loss int, hg_for int, hg_against int," +
      " away_played int, away_win int, away_draw int, away_loss int, ag_for int, ag_against int, points int) row format delimited fields terminated by ','")

    try{
      val input_file ="C:/Users/ghost/IdeaProjects/Premier_League_Tracker/src/main/Files/PLTable.json"
      val json_content = scala.io.Source.fromFile(input_file).mkString
      //print(table.mkString)

      val json_data = ujson.read(json_content)
      var trav = json_data("response")(0)("league")("standings")(0)
      var array = trav.arr.size
      //var obj = ujson.read(trav)
      for(i <- 0 to array - 1){
        spark.sql(s"INSERT INTO PL_TABLE VALUES('${trav(i)("team")("id")}', '${trav(i)("rank")}','${trav(i)("team")("name")}', '${trav(i)("all")("played")}', '${trav(i)("all")("win")}', '${trav(i)("all")("draw")}', '${trav(i)("all")("lose")}', '${trav(i)("all")("goals")("for")}', '${trav(i)("all")("goals")("against")}'" +
          s", '${trav(i)("home")("played")}', '${trav(i)("home")("win")}', '${trav(i)("home")("draw")}', '${trav(i)("home")("lose")}', '${trav(i)("home")("goals")("for")}', '${trav(i)("home")("goals")("against")}', '${trav(i)("away")("played")}'" +
          s", '${trav(i)("away")("win")}', '${trav(i)("away")("draw")}', '${trav(i)("away")("lose")}', '${trav(i)("away")("goals")("for")}', '${trav(i)("away")("goals")("against")}', '${trav(i)("points")}')")
      }

    }
    catch{
      case e:FileNotFoundException => println("File not found")
      case _:Exception => println("Error")
    }
    spark.sql("drop table if exists Players")
    spark.sql("drop table if exists Players_temp")
    spark.sql("create table IF NOT EXISTS Players_temp(player_id int, name string, nationality string, played int, minutes int," +
      "position string, shots int, goals int, goals_conceded int, assists int, passes int, key_passes int, accuracy int," +
      "y_cards int, r_cards int, id int) row format delimited fields terminated by ','")

    val teams = spark.sql("SELECT id FROM PL_Table").collect()
    teams.foreach(x => spark.sql(s"LOAD DATA LOCAL INPATH 'C:/Users/ghost/IdeaProjects/Premier_League_Tracker/src/main/Files/Players${x.get(0).toString}.csv' INTO TABLE Players_temp"))

    spark.sql("drop table if exists Players")
    spark.sql("create table IF NOT EXISTS Players(player_id int, name string, nationality string, played int, minutes int," +
      "position string, shots int, goals int, goals_conceded int, assists int, passes int, key_passes int, accuracy int," +
      "y_cards int, r_cards int) PARTITIONED BY (id int) row format delimited fields terminated by ','")
    spark.sql("INSERT OVERWRITE TABLE Players SELECT * FROM Players_temp")
    spark.sql("drop table if exists Players_temp")






  }
  def create_admin(username:String, pass:String): Unit ={
    try{
      val code= (100000 + Random.nextInt(900000))
      var response = spark.sql(s"SELECT * FROM users WHERE username = '$username'").collect()
      if(response.length == 0){
        spark.sql(s"INSERT INTO users VALUES('$code', '$username', '$pass','$username', '', '-1', 'admin')")
        println("Account successfully created")
      }else{
        println("username already exists")
      }
    }catch{
      case e: Exception => print("Error")
    }

  }
  def show_admins(): Unit ={
    spark.sql("SELECT * FROM users WHERE type='admin'").show()
  }
  def create_account(id:Int, firstName:String, lastName:String, username:String, pass:String) = {
    try{
      var response = spark.sql(s"SELECT * FROM users WHERE username = '$username'").collect()
      if(response.length == 0){
        spark.sql(s"INSERT INTO users VALUES('$id', '$username', '$pass','$firstName', '$lastName', '-1', 'user')")
        println("Account successfully created")
      }else{
        println("username already exists")
      }
    }catch{
      case e: Exception => print("Error")
    }


  }
  def sign_in(username:String, pass:String):Customer = {
    try{
      var response = spark.sql(s"SELECT * FROM users WHERE username = '$username' AND password = '$pass'").collect()
      if(response.length == 1){
        var id = 0
        var fn = ""
        var ln = ""
        var team_id = 0
        var user_t = ""

        response.foreach(row => {
          id = row.get(0).toString.toInt
          fn = row.get(3).toString
          ln = row.get(4).toString
          team_id = row.get(5).toString.toInt
          user_t = row.get(6).toString

        })

        new Customer(fn, ln, id, team_id, user_t)
      }else{
        println("User not found")
        null
      }
    }catch{
      case e: Exception => print("Error")
        null
    }


  }
  def drop(): Unit ={
    spark.sql("drop table if exists PL_Table")
    spark.sql("drop table if exists Players")
  }
  def change_team(id:Int , u_id:Int): Boolean ={
    try{
      val res = spark.sql(s"SELECT id FROM PL_Table WHERE id = '$id'").collect()
      if(res.length == 1){
        val user = spark.sql(s"SELECT * FROM users WHERE id = '$u_id'").collect()
        spark.sql(s"INSERT OVERWRITE TABLE users SELECT * FROM users WHERE id!='$u_id'")
        user.foreach(row =>{
          spark.sql(s"INSERT INTO users VALUES('${row.get(0).toString}', '${row.get(1).toString}','${row.get(2).toString}', '${row.get(3).toString}', '${row.get(4).toString}', '$id', '${row.get(6).toString}')")

        })
        true
      }else{
        false
      }
    }catch{
      case e:Exception => {println("Error occured")
        false}
    }

  }
  def get_teams(): Unit = {
    val response = spark.sql("SELECT id, team from PL_Table").collect()
    response.foreach(row => { println(s"${row.get(0).toString}. ${row.get(1).toString}")
    })

  }
  def get_table(): Unit ={
    spark.sql("SELECT rank as Position, team as Team, points as Pts, played as P, win as W, draw as D, loss as L FROM PL_Table ORDER BY Position").show()
  }
  def insert_player(): Unit ={
    val teams = spark.sql("SELECT id FROM PL_Table").collect()
    teams.foreach(x => API.getPlayers(x.get(0).toString.toInt))
  }
  def get_stats(id:Int): Unit ={
    val stats = spark.sql(s"SELECT team, rank, played, win, draw, loss, ROUND(win/played*100, 2) as win_p, goalsFor, goalsAgainst," +
      s" ROUND(goalsFor/played, 2) as goals_p, ROUND(goalsAgainst/played, 2) as goals_a, home_played, home_win, home_draw, home_loss, " +
      s" ROUND(home_win/home_played*100, 2) as h_win, hg_for, hg_against, ROUND(hg_for/home_played, 2) as goals_hf, ROUND(hg_against/home_played, 2) as goals_ha," +
      s"away_played, away_win, away_draw, away_loss, " +
      s" ROUND(away_win/home_played*100, 2) as a_win, ag_for, ag_against, ROUND(ag_for/home_played, 2) as goals_hf, ROUND(ag_against/home_played, 2) as goals_ha, points  FROM PL_Table WHERE id='$id'").collect()
    stats.foreach(x => {
      println("Team Name: "+x.get(0))
      println("League Table Position: "+x.get(1))
      println("\nGames Statistics")
      println("Games Played: "+x.get(2))
      println("Wins: "+x.get(3))
      println("Draws: "+x.get(4))
      println("Loss: "+x.get(5))
      println("Win Percentage: "+x.get(6)+"%")
      println("Goals Scored: "+x.get(7))
      println("Goals Conceded: "+x.get(8))
      println("Goals Scored Per Game: "+x.get(9))
      println("Goals Conceded Per Game: "+x.get(10))
      println("\nHome Games Statistics")
      println("Games Played: "+x.get(11))
      println("Wins: "+x.get(12))
      println("Draws: "+x.get(13))
      println("Loss: "+x.get(14))
      println("Win Percentage: "+x.get(15)+"%")
      println("Goals Scored: "+x.get(16))
      println("Goals Conceded: "+x.get(17))
      println("Goals Scored Per Game: "+x.get(18))
      println("Goals Conceded Per Game: "+x.get(19))
      println("\nAway Games Statistics")
      println("Games Played: "+x.get(20))
      println("Wins: "+x.get(21))
      println("Draws: "+x.get(22))
      println("Loss: "+x.get(23))
      println("Win Percentage: "+x.get(24)+"%")
      println("Goals Scored: "+x.get(25))
      println("Goals Conceded: "+x.get(26))
      println("Goals Scored Per Game: "+x.get(27))
      println("Goals Conceded Per Game: "+x.get(28))


    })

  }
  def get_team_players(id:Int): Unit ={
    val response = spark.sql(s"SELECT id, team from PL_Table where id ='$id'").collect()
    response.foreach(x => println("\n"+x.get(1)+" Players:"))
    println("\nDefenders: ")
    spark.sql(s"SELECT name as Name, nationality as Nationality, played as `Games Played`, minutes as `Minutes Played`," +
      s"shots as `Total Shots`, goals as Goals, ROUND(goals/shots*100, 2) as `Conversion Rate`, assists as Assists, passes as `Passes Made`, " +
      s"key_passes as `Key Passes`, accuracy as `Pass Accuracy`, y_cards as `Yellow Cards`, r_cards as `Red Cards` FROM Players WHERE id='$id' AND position LIKE '%Defender%' ORDER BY minutes DESC").show()
    println("Midfielders: ")
    spark.sql(s"SELECT name as Name, nationality as Nationality, played as `Games Played`, minutes as `Minutes Played`," +
      s"shots as `Total Shots`, goals as Goals, ROUND(goals/shots*100, 2) as `Conversion Rate`, assists as Assists, passes as `Passes Made`, " +
      s"key_passes as `Key Passes`, accuracy as `Pass Accuracy`, y_cards as `Yellow Cards`, r_cards as `Red Cards` FROM Players WHERE id='$id' AND position LIKE '%Midfielder%' ORDER BY minutes DESC").show()

    println("Attackers: ")
     spark.sql(s"SELECT name as Name, nationality as Nationality, played as `Games Played`, minutes as `Minutes Played`," +
      s"shots as `Total Shots`, goals as Goals, ROUND(goals/shots*100, 2) as `Conversion Rate`, assists as Assists, passes as `Passes Made`, " +
      s"key_passes as `Key Passes`, accuracy as `Pass Accuracy`, y_cards as `Yellow Cards`, r_cards as `Red Cards` FROM Players WHERE id='$id' AND position LIKE '%Attacker%' ORDER BY minutes DESC").show()
    println("Goalkeepers: ")
    spark.sql(s"SELECT name as Name, nationality as Nationality, played as `Games Played`, minutes as `Minutes Played`," +
      s"shots as `Total Shots`, goals as Goals, ROUND(goals/shots*100, 2) as `Conversion Rate`, assists as Assists, passes as `Passes Made`, " +
      s"key_passes as `Key Passes`, accuracy as `Pass Accuracy`, y_cards as `Yellow Cards`, r_cards as `Red Cards`  FROM Players WHERE id='$id' AND position LIKE '%Goalkeeper%'  ORDER BY minutes DESC").show()




  }
  def champions_league(): Unit ={
    println("Qualified for Champions League")
    spark.sql("SELECT rank as `League Table Position`, team as `Team Name`, points as `Total Points` FROM PL_Table WHERE rank < 5 ORDER BY RANK ").show()
    println("Qualified for Europa League")
    spark.sql("SELECT rank as `League Table Position`, team as `Team Name`, points as `Total Points` FROM PL_Table WHERE rank > 4 AND rank < 7 ORDER BY RANK ").show()
    println("Qualified for Europa Conference League")
    spark.sql("SELECT rank as `League Table Position`, team as `Team Name`, points as `Total Points` FROM PL_Table WHERE rank = 7 ORDER BY RANK ").show()
  }
  def relegation_zone(): Unit ={
    spark.sql("SELECT rank as `League Table Position`, team as `Team Name`, points as `Total Points` FROM PL_Table where rank > 17 ORDER BY RANK  ").show()
  }
  def change_password(id:Int, pass:String, npass:String): Unit={
    try{
      var response = spark.sql(s"SELECT * FROM users WHERE id = '$id' AND password ='$pass'").collect()
      if(response.length == 1){
        spark.sql(s"INSERT OVERWRITE TABLE users SELECT * FROM users WHERE id!='$id'")
        response.foreach(row=>{
          spark.sql(s"INSERT INTO users VALUES('${row.get(0).toString}', '${row.get(1).toString}','${npass}', '${row.get(3).toString}', '${row.get(4).toString}', '${row.get(5).toString}', '${row.get(6).toString}')")

        })

        println("Account successfully updated")
      }else{

        println("User not found")

      }
    }catch{
      case e: Exception => print("Error")
    }
  }
  def change_username(id:Int, u:String): Unit ={
    try{
      var response = spark.sql(s"SELECT * FROM users WHERE username ='$u'").collect()
      var user = spark.sql(s"SELECT * FROM users WHERE id='$id'").collect()
      if(response.length == 0){
        spark.sql(s"INSERT OVERWRITE TABLE users SELECT * FROM users WHERE id!='$id'")
        user.foreach(row =>{
          spark.sql(s"INSERT INTO users VALUES('${row.get(0).toString}', '${u}','${row.get(2).toString}', '${row.get(3).toString}', '${row.get(4).toString}', '${row.get(5).toString}', '${row.get(6).toString}')")

        })

        println("Account successfully updated")

      }else{
        println("Username already exists")
      }
    }catch{
      case e:Exception => print("Error")
    }

  }
  def delete_account(id:Int): Unit ={
    try{
      spark.sql(s"INSERT OVERWRITE TABLE users SELECT * FROM users WHERE id!='$id'")
    }catch {
      case e:Exception => {
        println("Error")
      }
    }

  }

}
