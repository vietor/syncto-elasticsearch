import syncd.utils._
import syncd.engine._

object RunMain {

  case class ConfigFile(
    port: Int,
    data: String,
    batch_size_mb: Int = 0,
    batch_queue_size: Int = 0,
    interval_oplog_ms: Int = 0,
    interval_retry_ms: Int = 0
  )

  val defaultSyncdConfig = SyncdConfig(
    batchSizeMB = 10,
    batchQueueSize = 1000,
    intervalOplogMS = 500,
    intervalRetryMS = 1000
  )

  def main(args: Array[String]): Unit = {
    val log4jFilePath = SomeUtil.tryFindFile(Array("", "config/"), "log4j2.xml")
    if(log4jFilePath != null)
      System.setProperty("log4j.configurationFile", log4jFilePath);

    val configText = SomeUtil.readFileAsString(Array("", "config/"), "config.json")
    if(configText == null)
      throw new IllegalStateException("Not found config.json")

    val config = JsonUtil.readValue(configText, classOf[ConfigFile])
    var restService = new RestManager(config.port, SyncdConfig(
      batchSizeMB = if (config.batch_size_mb > 0) config.batch_size_mb else defaultSyncdConfig.batchSizeMB,
      batchQueueSize = if (config.batch_queue_size > 0) config.batch_queue_size else defaultSyncdConfig.batchQueueSize,
      intervalOplogMS = if (config.interval_oplog_ms > 0) config.interval_oplog_ms else defaultSyncdConfig.intervalOplogMS,
      intervalRetryMS = if (config.interval_retry_ms > 0) config.interval_retry_ms else defaultSyncdConfig.intervalRetryMS,
    ), KtVtStore.openOrCreate({
      if(config.data == null || config.data.isEmpty())
        "data/"
      else
        config.data
    }))
    restService.start()
    sys.ShutdownHookThread {
      restService.stop();
    }
  }
}
