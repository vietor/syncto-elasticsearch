package syncd.engine

import java.util.Map

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

  def setStatus(status: Int): Unit = {
    currentStatus = status
  }

  def start(): Unit
  def stop(): Unit

  def dumpSummary(): Map[String, Map[String, String]]
}
