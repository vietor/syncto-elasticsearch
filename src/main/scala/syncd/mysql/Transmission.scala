package syncd.mysql

import java.util.{ArrayList, HashMap}
import scala.util.Using
import scala.jdk.CollectionConverters._

import org.bson.BasicBSONObject
import java.sql.{Connection, ResultSet}

object MyTransmission {
  case class ImportContext(
    config: MyConfig,
    columnTypes: HashMap[String, Int]
  )

  case class OplogContext(
    config: MyConfig,
    oplogName: String
  )

  private def readResultSetValue(columnTypes: HashMap[String, Int], key: String, rs: ResultSet): Any = {
    columnTypes.get(key) match {
      case n if n == MyConstants.TYPE_LONG => rs.getLong(key)
      case n if n == MyConstants.TYPE_FLOAT => rs.getDouble(key)
      case _ => rs.getString(key)
    }
  }

  def createImportContext(cluster: MyClusterNode, config: MyConfig): ImportContext = {
    ImportContext(config, cluster.getServerNode().columnTypes)
  }

  def importCollection(context: ImportContext, retry:()=> Int, iterate: (MyRecord) => Unit): Unit = {
    val config = context.config
    val columnTypes = context.columnTypes
    val columnNames = Array(config.table_pkey) ++ config.include_fields.asScala.toArray

    var inProgress: Boolean = true
    Using.resource(MyClientUtils.createClient(config.server)) { conn => {
      Using.resource(conn.createStatement()) { stmt => {
        Using.resource(stmt.executeQuery("SELECT " + columnNames.mkString(",") + " FROM " + config.table)) { rs => {
          while(rs.next()) {
            val doc = new BasicBSONObject()
            config.include_fields.forEach((key) => {
              doc.put(key, readResultSetValue(columnTypes, key, rs))
            })
            iterate(MyRecord(readResultSetValue(columnTypes, config.table_pkey, rs), doc))
          }
        }}
      }}
    }}

  }
}
