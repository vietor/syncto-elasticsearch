package syncd.mongodb

import org.bson.BSONObject
import java.util.{ArrayList}

import syncd.utils.{Validate}

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
  include_fields: ArrayList[String] = null
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
  id: Any,
  doc: BSONObject
)

case class MgOpRecord(
  shard: String,
  ts: MgTimestamp,
  op: Int,
  id: Any = null,
  doc: BSONObject = null
)

object MgValidator {

  private def validate(auth: MgAuth): Unit = {
    val PREFIX = "[mongodb.cluster.auth] bad field: "

    if(Validate.isNullOrBlank(auth.username))
      throw new IllegalStateException(PREFIX + "username")

    if(Validate.isNullOrBlank(auth.password))
      throw new IllegalStateException(PREFIX + "password")

    if(Validate.isNullOrBlank(auth.database))
      throw new IllegalStateException(PREFIX + "database")
  }

  private def validate(server: MgServer): Unit = {
    val PREFIX = "[mongodb.cluster.servers?] bad field: "

    if(Validate.isNullOrBlank(server.host))
      throw new IllegalStateException(PREFIX + "host")

    if(server.port < 1)
      throw new IllegalStateException(PREFIX + "port")
  }

  private def validate(cluster: MgCluster): Unit = {
    val PREFIX = "[mongodb.cluster] bad field: "

    if(cluster.auth != null)
      validate(cluster.auth)

    if(Validate.isNullOrEmpty(cluster.servers))
      throw new IllegalStateException(PREFIX + "servers")
    else
      cluster.servers.forEach(validate)
  }

  def validate(config: MgConfig): Unit = {
    val PREFIX = "[mongodb] bad field: "

    if(config.cluster != null)
      validate(config.cluster)
    else
      throw new IllegalStateException(PREFIX + "cluster")

    if(Validate.isNullOrBlank(config.db))
      throw new IllegalStateException(PREFIX + "db")

    if(Validate.isNullOrBlank(config.collection))
      throw new IllegalStateException(PREFIX + "collection")
  }
}
