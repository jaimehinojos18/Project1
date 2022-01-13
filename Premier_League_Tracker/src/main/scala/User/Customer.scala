package User

class Customer (private var firstName:String, private var lastName:String, private var id:Int, private var team_id:Int, private var user_type:String){
  def this(f:String, l:String, id:Int, user_t:String){
    this(f, l, id, -1, user_t)
  }
  def this(){
    this("", "", 0,"")
  }
  def getFirstName(): String ={
    firstName
  }
  def getLastName(): String={
    lastName
  }
  def getId(): Int ={
    id
  }
  def getTeamId(): Int ={
    team_id
  }
  def getUser(): String ={
    user_type
  }
  def setTeam(id:Int): Unit = {
    this.team_id = id
  }

}
