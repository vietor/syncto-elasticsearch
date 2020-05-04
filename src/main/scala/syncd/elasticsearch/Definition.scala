package syncd.elasticsearch

import java.util.{ArrayList}

import syncd.utils.{Validate, JsonUtil}

case class EsServer(
  host: String,
  port: Int
)

case class EsCluster(
  servers: ArrayList[EsServer]
)

case class EsCreator(
  mapping: String = null,
  settings: String = null
)

case class EsConfig(
  cluster: EsCluster,
  index: String,
  creator: EsCreator = null
)

case class EsRequest(
  op: Int,
  id: String,
  doc: String = null
)

object EsValidator {

  private def isJsonText(s: String): Boolean = {
    try {
      JsonUtil.readTree(s)
      true
    } catch {
      case _: Throwable => false
    }
  }

  private def validate(server: EsServer): Unit = {
    val PREFIX = "[elasticsearch.cluster.servers?] bad field: "

    if(Validate.isNullOrBlank(server.host))
      throw new IllegalStateException(PREFIX + "host")

    if(server.port < 1)
      throw new IllegalStateException(PREFIX + "port")
  }

  private def validate(cluster: EsCluster): Unit = {
    val PREFIX = "[elasticsearch.cluster] bad field: "

    if(Validate.isNullOrEmpty(cluster.servers))
      throw new IllegalStateException(PREFIX + "servers")
    else
      cluster.servers.forEach(validate)
  }

  private def validate(creator: EsCreator): Unit = {
    val PREFIX = "[elasticsearch.creator] bad field: "

    if(creator.mapping != null && !isJsonText(creator.mapping))
      throw new IllegalStateException(PREFIX + "mapping")

    if(creator.settings != null && !isJsonText(creator.settings))
      throw new IllegalStateException(PREFIX + "settings")
  }

  def validate(config: EsConfig): Unit = {
    val PREFIX = "[elasticsearch] bad field: "

    if(config.cluster != null)
      validate(config.cluster)
    else
      throw new IllegalStateException(PREFIX + "cluster")

    if(Validate.isNullOrBlank(config.index))
      throw new IllegalStateException(PREFIX + "index")

    if(config.creator != null)
      validate(config.creator)
  }
}
