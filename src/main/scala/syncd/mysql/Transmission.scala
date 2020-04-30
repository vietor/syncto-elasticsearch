package syncd.mysql

import java.util.{ArrayList, HashMap}
import scala.util.Using
import scala.jdk.CollectionConverters._

import org.bson.BasicBSONObject
import java.sql.{Connection, ResultSet}
import com.alibaba.otter.canal.protocol.CanalEntry._;

object MyTransmission {
  case class ImportContext(
    config: MyConfig,
    columnTypes: HashMap[String, Int]
  )

  case class OplogContext(
    config: MyConfig,
    columnTypes: HashMap[String, Int],
    canalProvider: CanalProvider
  )

  def createImportContext(cluster: MyClusterNode, config: MyConfig): ImportContext = {
    ImportContext(
      config,
      cluster.getServerNode().columnTypes
    )
  }

  def importCollection(context: ImportContext, retry:()=> Int, iterate: (MyRecord) => Unit): Unit = {
    val config = context.config

    def readValue(key: String, rs: ResultSet): Any = {
      if(rs.wasNull())
        null
      else
        context.columnTypes.get(key) match {
          case n if n == MyConstants.TYPE_LONG => rs.getLong(key)
          case n if n == MyConstants.TYPE_FLOAT => rs.getDouble(key)
          case _ => rs.getString(key)
        }
    }

    val columnNames  = Array(config.table_pkey) ++ config.include_fields.asScala.toArray

    Using.resource(MyClientUtils.createClient(config.server)) { conn => {
      Using.resource(conn.createStatement()) { stmt => {
        Using.resource(stmt.executeQuery("SELECT " + columnNames.mkString(",") + " FROM " + config.table)) { rs => {
          while(rs.next()) {
            val doc = new BasicBSONObject()
            config.include_fields.forEach((key) => {
              doc.put(key, readValue(key, rs))
            })
            iterate(MyRecord(readValue(config.table_pkey, rs), doc))
          }
        }}
      }}
    }}
  }

  def createOplogContext(cluster: MyClusterNode, config: MyConfig): OplogContext = {
    OplogContext(
      config,
      cluster.getServerNode().columnTypes,
      new CanalProvider(config)
    )
  }

  def releaseOplogContext(context: OplogContext): Unit = {
    context.canalProvider.release()
  }

  def syncCollectionOplog(context: OplogContext, timestamp: MyTimestamp, iterate: (MyOpRecord) => Unit): Unit = {
    val config = context.config

    def readValue(key: String, column: Column): Any = {
      if (column.getIsNull())
        null
      else
        context.columnTypes.get(key) match {
          case n if n == MyConstants.TYPE_LONG => column.getValue().toLong
          case n if n == MyConstants.TYPE_FLOAT => column.getValue().toDouble
          case _ => column.getValue()
        }
    }

    context.canalProvider.dump(timestamp, (timestamp: MyTimestamp, rowChange: RowChange) => {
      val opRecord = {
        if(rowChange != null)
          MyOpRecord(timestamp, MyConstants.OP_IGNORE)
        else {
          val eventType = rowChange.getEventType()
          MyOpRecord(timestamp, eventType match {
            case EventType.INSERT => MyConstants.OP_INSERT
            case EventType.UPDATE => MyConstants.OP_UPDATE
            case EventType.DELETE => MyConstants.OP_DELETE
            case _ => MyConstants.OP_IGNORE
          }, new ArrayList[MyRecord]() {
            for (rowData <- rowChange.getRowDatasList().asScala) {
              var columnsList = {
                if (eventType == EventType.DELETE) {
                  rowData.getBeforeColumnsList()
                } else {
                  rowData.getAfterColumnsList()
                }
              }

              var id: Any = null
              val doc = new BasicBSONObject()
              for(column <- columnsList.asScala) {
                val name = column.getName()
                if (config.table_pkey == name) {
                  id = readValue(name, column)
                } else if(config.include_fields contains name) {
                  doc.put(name, readValue(name, column))
                }
              }

              add(MyRecord(id, doc))
            }
          })
        }
      }
      iterate(opRecord)

      true
    })
  }
}
