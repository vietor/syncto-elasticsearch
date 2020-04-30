package syncd.mysql

import java.net.InetSocketAddress;

import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.alibaba.otter.canal.protocol.CanalEntry._;
import com.alibaba.otter.canal.parse.inbound._;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync._;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;

class CanalProvider(config: MyConfig) {
  var mainConnection: MysqlConnection = null
  var metaConnection: MysqlConnection = null
  var tableMetaCache: TableMetaCache = null
  var binlogParser: LogEventConvert = null

  private def active(): Unit = {
    if(mainConnection == null) {
      mainConnection = new MysqlConnection(new InetSocketAddress(config.server.host, config.server.port), config.server.user, config.server.password);
      mainConnection.connect()
    }
    if(!mainConnection.isConnected()) {
      mainConnection.reconnect()
    }

    if(metaConnection == null) {
      metaConnection = mainConnection.fork()
      metaConnection.connect()
    }
    if(!metaConnection.isConnected()) {
      metaConnection.reconnect()
    }

    if(tableMetaCache == null) {
      tableMetaCache = new TableMetaCache(metaConnection, null);
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
    if(metaConnection != null) {
      metaConnection.disconnect()
    }
    if (tableMetaCache != null) {
      tableMetaCache.clearTableMeta();
    }
    if(mainConnection != null) {
      mainConnection.disconnect()
    }
  }

  def dump(start: MyTimestamp, iterate: (MyTimestamp,RowChange) => Boolean): Unit = {
    active()

    mainConnection.dump(start.file, start.position, new SinkFunction[LogEvent]() {
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
}
