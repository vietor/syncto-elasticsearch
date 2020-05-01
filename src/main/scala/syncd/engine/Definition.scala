package syncd.engine

object Status {
  val UNKNOW = 0
  val STARTING = 1
  val START_FAILED = 2
  val RUNNING = 3
  val STOPPED = 4
}

case class SyncdConfig(
  fetchSize: Int,
  batchSizeMB: Int,
  batchQueueSize: Int,
  intervalOplogMS: Int,
  intervalRetryMS: Int
)
