import mongodbsync.utils._
import mongodbsync.engine._

object MongodbSync {

  case class ConfigFile(
    port: Int,
    data: String,
    batch_size_mb: Int = 0,
    batch_queue_size: Int = 0,
    interval_oplog_ms: Int = 0,
    interval_retry_ms: Int = 0
  )

  val defaultSyncConfig = SyncConfig(
    batchBytesMB = 10,
    batchQueueSize = 1000,
    intervalOplogMS = 500,
    intervalRetryMS = 1000
  )

  def main(args: Array[String]) {
    val log4jFilePath = SomeUtil.tryFindFile(Array("", "config/"), "log4j2.xml")
    if(log4jFilePath != null)
      System.setProperty("log4j.configurationFile", log4jFilePath);

    val configText = SomeUtil.readFileAsString(Array("", "config/"), "config.json")
    if(configText == null)
      throw new IllegalStateException("Not found config.json")

    val config = JsonUtil.readValue(configText, classOf[ConfigFile])
    var webService = new WebSynchronManager(config.port, SyncConfig(
      batchSizeMB = if (config.batch_size_mb > 0) config.batch_size_mb else defaultSyncConfig.batchSizeMB,
      batchQueueSize = if (config.batch_queue_size > 0) config.batch_queue_size else defaultSyncConfig.batchQueueSize,
      intervalOplogMS = if (config.interval_oplog_ms > 0) config.interval_oplog_ms else defaultSyncConfig.intervalOplogMS,
      intervalRetryMS = if (config.interval_retry_ms > 0) config.interval_retry_ms else defaultSyncConfig.intervalRetryMS,
    ), KtVtStore.openOrCreate({
      if(config.data == null || config.data.isEmpty())
        "data/"
      else
        config.data
    }))
    webService.start()
    sys.ShutdownHookThread {
      webService.stop();
    }
  }
}
