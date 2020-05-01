package syncd.mysql

import java.net.InetSocketAddress;

import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.alibaba.otter.canal.protocol.CanalEntry._;
import com.alibaba.otter.canal.parse.inbound._;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync._;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;

class CanalProvider(config: MyConfig) {
  var dumpConnection: MysqlConnection = null
  var metaConnection: MysqlConnection = null
  var tableMetaCache: TableMetaCache = null
  var binlogParser: LogEventConvert = null

  private def preMeta(): Unit = {
    if(metaConnection == null) {
      metaConnection = new MysqlConnection(
        new InetSocketAddress(config.server.host, config.server.port),
        config.server.user,
        config.server.password
      )
      metaConnection.connect()
    } else if(!metaConnection.isConnected()) {
      metaConnection.reconnect()
    }

    if(tableMetaCache == null) {
      tableMetaCache = new TableMetaCache(metaConnection, null);
    }
  }

  private def preDump(): Unit =  {
    if(dumpConnection == null) {
      dumpConnection = metaConnection.fork()
      dumpConnection.connect()
    } else {
      dumpConnection.reconnect()
    }

    if(binlogParser == null) {
      binlogParser = new LogEventConvert()
      binlogParser.setTableMetaCache(tableMetaCache);
      binlogParser.setFilterTableError(true)
      binlogParser.start()
    }
  }

  def release(): Unit = {
    if(binlogParser != null) {
      binlogParser.stop()
    }
    if(dumpConnection != null) {
      dumpConnection.disconnect()
    }
    if (tableMetaCache != null) {
      tableMetaCache.clearTableMeta();
    }
    if(metaConnection != null) {
      metaConnection.disconnect()
    }
  }

  private def getTimestamp(): MyTimestamp = {
    val columnValues = metaConnection.query("show master status")
      .getFieldValues()
    MyTimestamp(columnValues.get(0), columnValues.get(1).toLong)
  }

  private def canDump(start: MyTimestamp): Boolean = {
    preMeta()

    getTimestamp() != start
  }

  private def runDump(start: MyTimestamp, iterate: (MyTimestamp,RowChange) => Boolean): Unit = {
    preDump()

    dumpConnection.dump(start.file, start.position, new SinkFunction[LogEvent]() {
      def sink(event: LogEvent): Boolean = {
        val timestamp = MyTimestamp(
          event.getHeader().getLogFileName(),
          event.getLogPos()
        )
        var rowChange: RowChange = null
        val entry = binlogParser.parse(event, true)
        if(entry != null) {
          val header = entry.getHeader()

          if(header.getSchemaName() == config.server.database
            && header.getTableName()== config.table
            && entry.getEntryType() == EntryType.ROWDATA) {
            rowChange = RowChange.parseFrom(entry.getStoreValue())
          }
        }
        iterate(timestamp, rowChange)
      }
    })
  }

  def dump(start: MyTimestamp, iterate: (MyTimestamp,RowChange) => Boolean): Unit = {
    if(canDump(start)) {
      runDump(start, iterate)
    }
  }
}
