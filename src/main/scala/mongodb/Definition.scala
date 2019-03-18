package mongodbsync.mongodb

import java.util.ArrayList
import com.mongodb.DBObject

case class MgServer(
  host: String,
  port: Int
)

case class MgAuth(
  username: String,
  database: String,
  password: String
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
  id: Object = null,
  doc: DBObject = null
)
