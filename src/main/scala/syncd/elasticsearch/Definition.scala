package syncd.elasticsearch

import java.util.ArrayList

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
