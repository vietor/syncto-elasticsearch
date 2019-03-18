package mongodbsync.engine

import java.util.Map

object Status {
  val UNKNOW = 0
  val STARTING = 1
  val START_FAILED = 2
  val RUNNING = 3
  val STOPPED = 4
}

case class SyncConfig(
  batchActions: Int,
  batchBytesMB: Int,
  intervalOplogMS: Int,
  intervalRetryMS: Int
)

trait AbstractSync {
  private var currentStatus = Status.UNKNOW

  def getStatus(): Int = {
    currentStatus
  }

  def getStatusText(): String = {
    currentStatus match {
      case Status.STARTING => "STRTING"
      case Status.START_FAILED => "START_FAILED"
      case Status.RUNNING => "RUNNING"
      case Status.STOPPED => "STOPPED"
      case _ => "UNKNOW"
    }
  }

  def setStatus(status: Int) {
    currentStatus = status
  }

  def start()
  def stop()

  def dumpSummary(): Map[String, Map[String, String]]
}
