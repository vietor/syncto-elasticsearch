package syncd.mongodb

import org.bson.BSONObject
import java.util.{ArrayList}

case class MgServer(
  host: String,
  port: Int
)

case class MgAuth(
  username: String,
  password: String,
  database: String
)

case class MgCluster(
  auth: MgAuth = null,
  servers: ArrayList[MgServer]
)

case class MgConfig(
  cluster: MgCluster,
  db: String,
  collection: String,
  include_fields: ArrayList[String]
)

case class MgTimestamp(
  time: Long,
  seq: Long
)

case class MgShardNode(
  name: String,
  replicas: ArrayList[MgServer],
  timestamp: MgTimestamp,
  oplogName: String
)

case class MgServerNode(
  isMongos: Boolean,
  shards: ArrayList[MgShardNode]
)

case class MgRecord(
  id: Object,
  doc: DBObject
)

case class MgOpRecord(
  ts: MgTimestamp,
  op: Int,
  id: Any = null,
  doc: BSONObject = null
)
