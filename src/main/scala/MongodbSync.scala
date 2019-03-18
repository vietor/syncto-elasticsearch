import mongodbsync.utils._
import mongodbsync.engine._

object MongodbSync {

  case class ConfigFileStruct(
    port: Int,
    data: String
  )

  def main(args: Array[String]) {
    val log4jFilePath = SomeUtil.tryFindFile(Array("", "config/"), "log4j2.xml")
    if(log4jFilePath != null)
      System.setProperty("log4j.configurationFile", log4jFilePath);

    val configText = SomeUtil.readFileAsString(Array("", "config/"), "config.json")
    if(configText == null)
      throw new IllegalStateException("Not found config.json")

    val config = JsonUtil.readValue(configText, classOf[ConfigFileStruct])
    var webService = new WebSynchronManager(config.port, KtVtStore.openOrCreate({
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
