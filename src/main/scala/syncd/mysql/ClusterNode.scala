package syncd.mysql

import java.util.{HashMap}
import scala.util.Using
import scala.jdk.CollectionConverters._

import java.sql.{Connection}

import syncd.utils.{Validate}

class MyClusterNode(config: MyConfig) {

  private val keyLong = Array("tinyint", "smallint", "int", "bigint")
  private val keyFloat = Array("float", "decimal")
  private val keyText = Array("varchar", "text")
  private val keyDate = Array("date", "datetime", "timestamp")

  private def getColumnTypes(conn: Connection): HashMap[String, Int] = {
    val columnNames = {
      if(Validate.isNullOrEmpty(config.include_fields))
        Array(config.table_pkey)
      else
        Array(config.table_pkey) ++ config.include_fields.asScala.toArray
    }

    Using.resource(conn.createStatement()) { stmt => {
      Using.resource(stmt.executeQuery("show columns from " + config.table)) { rs => {
        val types = new HashMap[String, Int]()
        while(rs.next()) {
          val name = rs.getString("Field")
          if(columnNames.length == 1 || columnNames.contains(name)) {
            val sType = {
              val text = rs.getString("Type")
              val pos = text.indexOf("(")
              if(pos < 0)
                text
              else
                text.substring(0, pos)
            }.replace("unsigned", "").trim()

            types.put(name, sType match {
              case x if keyLong.contains(x) => MyConstants.TYPE_LONG
              case x if keyFloat.contains(x) => MyConstants.TYPE_FLOAT
              case x if keyText.contains(x) => MyConstants.TYPE_TEXT
              case x if keyDate.contains(x) => MyConstants.TYPE_DATE
              case _ => throw new IllegalStateException("Unsupport column type: " + sType + " on " + name)
            })
          }
        }
        for(name <- columnNames) {
          if(!types.containsKey(name))
            throw new IllegalStateException("Not found column: " + name)
        }
        types
      }}
    }}
  }

  private def getMyTimestamp(conn: Connection): MyTimestamp = {
    Using.resource(conn.createStatement()) { stmt => {
      Using.resource(stmt.executeQuery("show master status")) { rs => {
        if(!rs.next())
          throw new IllegalStateException("Cannot found anything in `show master status`.")

        MyTimestamp(
          rs.getString("File"),
          rs.getLong("Position")
        )
      }}
    }}
  }

  private var serverNode = {
    Using.resource(MyClientUtils.createClient(config.server)) { conn => {
      MyServerNode(
        getMyTimestamp(conn),
        getColumnTypes(conn)
      )
    }}
  }

  def getServerNode(): MyServerNode = {
    serverNode
  }
}
