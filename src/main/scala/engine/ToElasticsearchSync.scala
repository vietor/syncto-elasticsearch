package mongodbsync.engine

import java.util.{Map, HashMap}
import java.util.ArrayList
import java.util.concurrent._
import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory
import org.elasticsearch.ElasticsearchStatusException

import mongodbsync.utils._
import mongodbsync.mongodb._
import mongodbsync.elasticsearch._

class ToElasticsearchSync(syncKey: String, mgConfig: MgConfig, esConfig: EsConfig, ktvtStore: KtVtCollection) extends AbstractSync {
  private val MAX_BULK_BYTESMB = 10
  private val MAX_BULK_ACTIONS = 1000
  private val INTERVAL_OPLOG = 500
  private val INTERVAL_NETWORK_RETRY = 10000
  private val logger = LoggerFactory.getLogger(getClass().getName())

  private def setStatusStep(step: String) {
    ktvtStore.put("status", "step", step)
  }

  private def readOplogTimestamp(shard: MgShardNode): MgTimestamp = {
    try {
      JsonUtil.readValue(ktvtStore.get("oplog", shard.name, JsonUtil.writeValueAsString(shard.timestamp)), classOf[MgTimestamp])
    } catch {
      case _: Throwable => shard.timestamp
    }
  }

  private def storeOplogTimestamp(shard: MgShardNode, opTimestamp: MgTimestamp) {
    ktvtStore.put("oplog", shard.name, JsonUtil.writeValueAsString(opTimestamp))
  }

  private def storeOplogTimestamp(shard: String, opTimestamp: MgTimestamp) {
    ktvtStore.put("oplog", shard, JsonUtil.writeValueAsString(opTimestamp))
  }

  private case class OplogRecord(
    shard: String,
    record: MgOpRecord
  )

  private val oplogRecordQueue = new LinkedBlockingQueue[OplogRecord](MAX_BULK_ACTIONS)

  private class OplogThread(cluster: MgClusterNode, shard: MgShardNode) extends Runnable {
    val context = MgClusterNode.createOplogContext(cluster, shard, mgConfig)

    override def run() {
      var opTimestamp = readOplogTimestamp(shard)

      try {
        while(true) {
          var sleepMS = INTERVAL_OPLOG
          try {
            MgClusterNode.syncCollectionOplog(context, opTimestamp, (record: MgOpRecord) => {
              opTimestamp = record.ts
              oplogRecordQueue.put(OplogRecord(shard.name, record))
              MgConstants.RECORD_NEXT
            })
          } catch {
            case e: Throwable => {
              if(!MgClientUtils.isRetrySafety(e)) {
                if(MgClientUtils.isInterrupted(e))
                  throw e
                logger.error("[" + syncKey + "] Fetch oplog", e)
              }
              else
                sleepMS = INTERVAL_NETWORK_RETRY
            }
          }
          Thread.sleep(sleepMS)
        }
      } catch {
        case e: Throwable => {
          if(!MgClientUtils.isInterrupted(e))
            logger.error("[" + syncKey + "] Escape Oplog Thread", e)
        }
      }
    }
  }

  private class MainThread extends Runnable {

    private class ShardStatus() {
      var count = 0;
      var timestamp: MgTimestamp = null
    }

    override def run() {
      var mgCluster: MgClusterNode = null
      var esCluster: EsClusterNode = null
      var bulkProcessor: EsBulkProcessor = null
      val shardStatus = new HashMap[String, ShardStatus]

      try {
        setStatusStep("STARTUP")
        setStatus(Status.STARTING)

        var waitingStep = true
        while(waitingStep) {
          try {
            if(mgCluster == null)
              mgCluster = new MgClusterNode(mgConfig.cluster)
            if(esCluster == null)
              esCluster = new EsClusterNode(esConfig.cluster)
            waitingStep = false
          }
          catch {
            case e: Throwable => {
              if(!MgClientUtils.isRetrySafety(e)) {
                setStatus(Status.START_FAILED)
                throw e
              }
              Thread.sleep(INTERVAL_NETWORK_RETRY)
            }
          }
        }

        if(esConfig.creator != null) {
          if(esCluster.exists(esConfig.index))
            esCluster.modifyMapping(esConfig.index, esConfig.creator.mapping);
          else
            esCluster.create(esConfig.index, esConfig.creator.settings, esConfig.creator.mapping);
        }

        bulkProcessor = esCluster.createBulkProcessor(
          esConfig.index,
          EsBulkParameters(
            actions = MAX_BULK_ACTIONS,
            bytesOnMB = MAX_BULK_BYTESMB,
            flushIntervalOnMillis = INTERVAL_OPLOG,
            itemsErrorWatcher = (count: Int, e: Throwable) => {
              logger.error("[" + syncKey + "] Bulk items " + count, e)
            },
            globalErrorWatcher = (e: Throwable) => {
              logger.error("[" + syncKey + "] Bulk processor", e)
            }
          ))

        setStatus(Status.RUNNING)

        if(ktvtStore.get("import", "completed", "no") != "ok") {
          setStatusStep("IMPORT")

          var count:Long = 0
          val start_ts = SomeUtil.getTimestamp()
          MgClusterNode.importCollection(MgClusterNode.createImportContext(mgCluster, mgConfig), ()=> {
            INTERVAL_NETWORK_RETRY
          }, (record: MgRecord) => {
            count += 1
            bulkProcessor.index(record.id.toString, JsonUtil.writeValueAsString(record.doc))
            ktvtStore.put("import", "count", count.toString)
            ktvtStore.put("import", "duration", (SomeUtil.getTimestamp() - start_ts).toString)
            MgConstants.RECORD_NEXT
          })
          bulkProcessor.flush()
          val completed_ts = SomeUtil.getTimestamp
          ktvtStore.putAll("import", new HashMap[String, String]() {
            put("count", count.toString)
            put("duration", (completed_ts - start_ts).toString)
            put("completed", "ok")
            put("completed_ts", completed_ts.toString)
          })
          mgCluster.getServerNode().shards.forEach(shard =>
            storeOplogTimestamp(shard, shard.timestamp)
          )
        }

        mgCluster.getServerNode().shards.forEach(shard => {
          val thread = new Thread(new OplogThread(mgCluster, shard))
          thread.setDaemon(true)
          oplogThreads.add(thread)
          shardStatus.put(shard.name, new ShardStatus())
        })

        setStatusStep("OPLOG")
        oplogThreads.forEach(thread =>
          thread.start()
        )

      } catch {
        case e: Throwable => {
          setStatus(Status.START_FAILED)
          if(!MgClientUtils.isInterrupted(e))
            logger.error("[" + syncKey + "] Mongo to ElasticSearch Starting", e)
        }
      }

      if(getStatus() == Status.RUNNING) {

        var lastActionTS = bulkProcessor.getActionTS()
        while(true) {
          try {
            val oplogRecord = oplogRecordQueue.poll(INTERVAL_OPLOG, TimeUnit.MILLISECONDS)
            if(oplogRecord != null) {
              val record = oplogRecord.record
              val status = shardStatus.get(oplogRecord.shard)

              status.count += 1
              status.timestamp = record.ts

              record.op match {
                case MgConstants.OP_CREATE =>
                  bulkProcessor.index(record.id.toString, JsonUtil.writeValueAsString(record.doc))
                case MgConstants.OP_UPDATE =>
                  bulkProcessor.update(record.id.toString, JsonUtil.writeValueAsString(record.doc))
                case MgConstants.OP_DELETE =>
                  bulkProcessor.delete(record.id.toString)
                case MgConstants.OP_RECREATE =>
                  bulkProcessor.index(record.id.toString, JsonUtil.writeValueAsString(record.doc))
                case _ => {}
              }
            }
            if(oplogRecord == null)
              bulkProcessor.flush()

            val actionTS = bulkProcessor.getSystemTS()
            if(lastActionTS != actionTS) {
              lastActionTS = actionTS
              for((shard, status) <- shardStatus.asScala) {
                if(status.count > 0) {
                  status.count = 0
                  storeOplogTimestamp(shard, status.timestamp)
                }
              }
            }
          } catch {
            case e: Throwable => {
              if(!MgClientUtils.isInterrupted(e))
                logger.error("[" + syncKey + "] Poll Mongodb oplog to Elasticsearch", e)
            }
          }
        }
      }

      if(bulkProcessor != null)
        bulkProcessor.close()
    }
  }

  private var mainThread: Thread = null
  private val oplogThreads = new ArrayList[Thread]()

  override def start() {
    if(mainThread != null)
      throw new IllegalStateException("Sync already started")
    mainThread = new Thread(new MainThread())
    mainThread.setDaemon(true)
    mainThread.start()
  }

  override def stop() {
    try {
      if(mainThread != null) {
        mainThread.interrupt()
        mainThread = null
      }
      if(oplogThreads.size() > 0) {
        oplogThreads.forEach(thread =>
          thread.interrupt()
        )
        oplogThreads.clear()
      }
    }
    finally {
      setStatus(Status.STOPPED)
    }
  }

  override def dumpSummary(): Map[String, Map[String, String]] = {
    new HashMap[String, Map[String, String]]() {
      ktvtStore.getKeys().forEach(key =>
        put(key, ktvtStore.getAll(key))
      )
    }
  }
}
