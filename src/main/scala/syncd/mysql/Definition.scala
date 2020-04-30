package syncd.mysql

import org.bson.BSONObject
import java.util.{ArrayList,HashMap}

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

case class MgOpRecord(
  op: Int,
  id: Any = null,
  doc: BSONObject = null
)
