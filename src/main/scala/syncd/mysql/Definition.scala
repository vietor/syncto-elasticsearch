package syncd.mysql

import org.bson.BSONObject
import java.util.{ArrayList,HashMap}

import syncd.utils.{Validate}

case class MyServer(
  host: String,
  port: Int,
  user: String,
  password: String,
  database: String
)

case class MyConfig(
  server: MyServer,
  table: String,
  table_pkey: String,
  include_fields: ArrayList[String]
)

case class MyTimestamp(
  file: String,
  position: Long
)

case class MyServerNode(
  timestamp: MyTimestamp,
  columnTypes: HashMap[String, Int]
)

case class MyRecord(
  id: Any,
  doc: BSONObject
)

case class MyOpRecord(
  ts: MyTimestamp,
  op: Int,
  docs: ArrayList[MyRecord] = null
)

object MyValidator {

  private def validate(server: MyServer): Unit = {
    val PREFIX = "[mysql.server] bad field: "

    if(Validate.isNullOrBlank(server.host))
      throw new IllegalStateException(PREFIX + "host")

    if(server.port < 1)
      throw new IllegalStateException(PREFIX + "port")

    if(Validate.isNullOrBlank(server.user))
      throw new IllegalStateException(PREFIX + "user")

    if(Validate.isNullOrBlank(server.password))
      throw new IllegalStateException(PREFIX + "password")

    if(Validate.isNullOrBlank(server.database))
      throw new IllegalStateException(PREFIX + "database")
  }

  def validate(config: MyConfig): Unit = {
    val PREFIX = "[mysql] bad field: "

    if(config.server != null)
      validate(config.server)
    else
      throw new IllegalStateException(PREFIX + "server")

    if(Validate.isNullOrBlank(config.table))
      throw new IllegalStateException(PREFIX + "table")

    if(Validate.isNullOrBlank(config.table_pkey))
      throw new IllegalStateException(PREFIX + "table_pkey")
  }
}
