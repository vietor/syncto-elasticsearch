import mongodbsync.utils._
import mongodbsync.engine._

object MongodbSync {

  case class ConfigFile(
    port: Int,
    data: String
  )

  private val syncConfig = SyncConfig(
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
    var webService = new WebSynchronManager(config.port, syncConfig, KtVtStore.openOrCreate({
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
