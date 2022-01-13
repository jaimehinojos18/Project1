package Menus
import User.Customer

import scala.io.StdIn
import Database.spark_connect

import scala.util.Random
object sign_in {




  def signInMenu(i:Int, sp:spark_connect.type ): Customer =i  match{
    case 1 => {
      println("Please enter the following information: ")

      print("First Name: \n")
      val name = StdIn.readLine()
      print("Last Name: \n")
      val lastname = StdIn.readLine()
      print("Username: \n")
      val username = StdIn.readLine()
      print("Password: \n")
      val password = StdIn.readLine()
      val code= (100000 + Random.nextInt(900000))
      sp.create_account(code, name, lastname, username, password)
      println("\nMenu: ")
      println("1. Create Account \n2. Login \nq. quit\n " +
        "Please type what you would like to do Ex(To Create account input 1): ")

      null
    }

    case 2 => {
      print("Please enter your username: ")
      val username = StdIn.readLine()
      print("\nPlease enter your password: ")
      val password = StdIn.readLine()
      val result = sp.sign_in(username, password)
      if(result == null){
        println("\nMenu: ")
        println("1. Create Account \n2. Login \nq. quit\n " +
          "Please type what you would like to do Ex(To Create account input 1): ")
        null
      }else {
        result
      }
    }
    case _ => {
      println("\nMenu: ")
      println("1. Create Account \n2. Login \nq. quit\n " +
        "Please type what you would like to do Ex(To Create account input 1): ")
      null
    }

  }


}