package syncd.mysql

import java.sql.{DriverManager, Connection}

object MyClientUtils {

  def createClient(server: MyServer): Connection = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://" + server.host + ":" + server.port + "/" + server.database + "?user=" + server.user + "&password=" + server.password)
  }

  def isRetrySafety(e: Throwable): Boolean = {
    false
  }

  def isInterrupted(e: Throwable): Boolean = {
    false
  }

}
