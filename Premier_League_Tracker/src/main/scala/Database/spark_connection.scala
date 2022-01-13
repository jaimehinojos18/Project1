package Database
import org.apache.spark.sql.SparkSession
class spark_connection (){
  System.setProperty("hadoop.home.dir", "C:/hadoop")
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  def initiate(): Unit = {
    spark.sql("create table IF NOT EXISTS users (id int, username string, password string, firstName string, lastName string, type string) row format delimited fields terminated by ','")

  }
  def create_account(id:Int, firstName:String, lastName:String, username:String, pass:String) = {
    try{
      var response = spark.sql(s"SELECT * FROM users WHERE username = '$username'").collect()
      if(response.length == 0){
        spark.sql(s"INSERT INTO users VALUES('$id', '$username', '$pass','$firstName', '$lastName','-1','user')")
        println("Account successfully created")
      }else{
        println("username already exists")
      }
    }catch{
      case e: Exception => print("Error")
    }
    spark.sql(s"SELECT * FROM users").show()


  }
  def drop(): Unit ={
    spark.sql("drop table if exists users")
  }
}
