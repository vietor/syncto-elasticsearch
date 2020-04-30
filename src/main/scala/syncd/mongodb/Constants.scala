package syncd.mongodb

object MgConstants {
  val ADMIN_DATABASE = "admin"
  val LOCAL_DATABASE = "local"
  val CONFIG_DATABASE = "config"
  val SHARD_COLLECTION = "shards"
  val OPLOG_COLLECTION1 = "oplog.rs"
  val OPLOG_COLLECTION2 = "oplog.$main"

  val OP_IGNORE = 0
  val OP_INSERT = 1
  val OP_UPDATE = 2
  val OP_DELETE = 3
  val OP_REINSERT = 4
}
