import syncd.utils._
import syncd.engine._

object RunMain {

  case class ConfigFile(
    port: Int,
    data: String,
    fetch_size: Int = 0,
    batch_size_mb: Int = 0,
    batch_queue_size: Int = 0,
    interval_oplog_ms: Int = 0,
    interval_retry_ms: Int = 0
  )

  private val defaultSyncdConfig = SyncdConfig(
    fetchSize = 1000,
    batchSizeMB = 10,
    batchQueueSize = 1000,
    intervalOplogMS = 500,
    intervalRetryMS = 1000
  )

  private def fixValue(value: Int, defaultValue: Int): Int = if(value > 0) value else defaultValue

  def main(args: Array[String]): Unit = {
    val log4jFilePath = SomeUtil.tryFindFile(Array("", "config/"), "log4j2.xml")
    if(log4jFilePath != null)
      System.setProperty("log4j.configurationFile", log4jFilePath);

    val configText = SomeUtil.readFileAsString(Array("", "config/"), "config.json")
    if(configText == null)
      throw new IllegalStateException("Not found config.json")

    val config = JsonUtil.readValue(configText, classOf[ConfigFile])
    var restService = new RestManager(config.port, SyncdConfig(
      fetchSize = fixValue(config.fetch_size, defaultSyncdConfig.fetchSize),
      batchSizeMB = fixValue (config.batch_size_mb, defaultSyncdConfig.batchSizeMB),
      batchQueueSize = fixValue(config.batch_queue_size, defaultSyncdConfig.batchQueueSize),
      intervalOplogMS = fixValue(config.interval_oplog_ms, defaultSyncdConfig.intervalOplogMS),
      intervalRetryMS = fixValue(config.interval_retry_ms, defaultSyncdConfig.intervalRetryMS)
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
