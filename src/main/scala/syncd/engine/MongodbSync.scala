package syncd.engine

import java.util.concurrent._
import java.util.{Map, HashMap, ArrayList}
import scala.jdk.CollectionConverters._

import org.slf4j.LoggerFactory
import org.elasticsearch.ElasticsearchStatusException

import syncd.utils._
import syncd.mongodb._
import syncd.elasticsearch._

class MongodbSync(syncdConfig: SyncdConfig, syncKey: String,  mgConfig: MgConfig, esConfig: EsConfig, ktvtStore: KtVtCollection) extends AbstractSync {
  private val logger = LoggerFactory.getLogger(getClass().getName())

  private def setStatusStep(step: String): Unit = {
    ktvtStore.put("status", "step", step)
  }

  private def readOplogTimestamp(shard: String, opTimestamp: MgTimestamp): MgTimestamp = {
    try {
      JsonUtil.readValue(ktvtStore.get("oplog", shard, JsonUtil.writeValueAsString(opTimestamp)), classOf[MgTimestamp])
    } catch {
      case _: Throwable => opTimestamp
    }
  }

  private def storeOplogTimestamp(shard: String, opTimestamp: MgTimestamp): Unit = {
    ktvtStore.put("oplog", shard, JsonUtil.writeValueAsString(opTimestamp))
  }

  private val oplogRecordQueue = new LinkedBlockingQueue[MgOpRecord](syncdConfig.batchQueueSize)

  private class OplogThread(cluster: MgClusterNode, shard: MgShardNode) extends Runnable {
    val context = MgTransmission.createOplogContext(cluster, shard, mgConfig)

    override def run(): Unit = {
      var interrupted = false
      var opTimestamp = readOplogTimestamp(shard.name, shard.timestamp)

      logger.debug("[" + syncKey +  "] start oplog thread on " + shard.name)

      while(!interrupted) {
        var sleepMS = syncdConfig.intervalOplogMS
        try {
          MgTransmission.syncCollectionOplog(context, opTimestamp, (record: MgOpRecord) => {
            opTimestamp = record.ts
            oplogRecordQueue.put(record)
          })
        } catch {
          case e: Throwable => {
            if(MgClientUtils.isInterrupted(e))
              interrupted = true
            else if(MgClientUtils.isRetrySafety(e))
              sleepMS = syncdConfig.intervalRetryMS
            else
              logger.error("[" + syncKey + "] oplog fetch", e)
          }
        }
        if(!interrupted) {
          try {
            Thread.sleep(sleepMS)
          } catch {
            case _: Throwable => {
              interrupted = true
            }
          }
        }
      }

      logger.debug("[" + syncKey +  "] stop oplog thread on " + shard.name)
    }
  }

  private class MainThread extends Runnable {

    private class ShardStatus() {
      var count = 0;
      var timestamp: MgTimestamp = null
    }

    override def run(): Unit =  {
      var mgCluster: MgClusterNode = null
      var esCluster: EsClusterNode = null
      var bulkProcessor: EsBulkProcessor = null
      val shardStatus = new HashMap[String, ShardStatus]

      logger.debug("[" + syncKey + "] start main thread")

      try {
        setStatusStep("STARTUP")
        setStatus(Status.STARTING)

        while(mgCluster == null || esCluster == null) {
          try {
            if(mgCluster == null)
              mgCluster = new MgClusterNode(mgConfig.cluster)
            if(esCluster == null)
              esCluster = new EsClusterNode(esConfig.cluster)
          } catch {
            case e: Throwable => {
              if(!MgClientUtils.isRetrySafety(e))
                throw e
              else
                Thread.sleep(syncdConfig.intervalRetryMS)
            }
          }
        }

        if(esConfig.creator != null) {
          if(esCluster.exists(esConfig.index))
            esCluster.modifyMapping(esConfig.index, esConfig.creator.mapping);
          else
            esCluster.create(esConfig.index, esConfig.creator.settings, esConfig.creator.mapping);
        }

        bulkProcessor = esCluster.createBulkProcessor(esConfig.index, EsBulkParameters(
          actions = syncdConfig.batchQueueSize,
          bytesOnMB = syncdConfig.batchSizeMB,
          flushIntervalOnMillis = syncdConfig.intervalOplogMS,
          itemsErrorWatcher = (e: Throwable) => {
            logger.error("[" + syncKey + "] Bulk items", e)
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
          MgTransmission.importCollection(MgTransmission.createImportContext(mgCluster, mgConfig), ()=> {
            syncdConfig.intervalRetryMS
          }, (record: MgRecord) => {
            count += 1
            bulkProcessor.index(record.id.toString, JsonUtil.writeValueAsString(record.doc))
            ktvtStore.put("import", "count", count.toString)
            ktvtStore.put("import", "duration", (SomeUtil.getTimestamp() - start_ts).toString)
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
            storeOplogTimestamp(shard.name, shard.timestamp)
          )
        }

        setStatusStep("OPLOG")
        mgCluster.getServerNode().shards.forEach(shard => {
          shardStatus.put(shard.name, new ShardStatus())
          val thread = new Thread(new OplogThread(mgCluster, shard))
          thread.setDaemon(true)
          oplogThreads.add(thread)
        })
        oplogThreads.forEach(thread => thread.start())

      } catch {
        case e: Throwable => {
          setStatus(Status.START_FAILED)
          if(!MgClientUtils.isInterrupted(e))
            logger.error("[" + syncKey + "] import process", e)
        }
      }

      if(getStatus() == Status.RUNNING) {
        bulkProcessor.resetSync()

        var interrupted = false
        while(!interrupted) {
          try {
            val record = oplogRecordQueue.poll(syncdConfig.intervalOplogMS, TimeUnit.MILLISECONDS)
            if(record != null) {
              val status = shardStatus.get(record.shard)

              status.count += 1
              status.timestamp = record.ts

              record.op match {
                case MgConstants.OP_INSERT =>
                  bulkProcessor.index(record.id.toString, JsonUtil.writeValueAsString(record.doc))
                case MgConstants.OP_UPDATE =>
                  bulkProcessor.update(record.id.toString, JsonUtil.writeValueAsString(record.doc))
                case MgConstants.OP_DELETE =>
                  bulkProcessor.delete(record.id.toString)
                case MgConstants.OP_REINSERT =>
                  bulkProcessor.index(record.id.toString, JsonUtil.writeValueAsString(record.doc))
                case _ => {}
              }
            }
            if(record == null)
              bulkProcessor.flush()

            if(bulkProcessor.detectSync()) {
              for((shard, status) <- shardStatus.asScala) {
                if(status.count > 0) {
                  status.count = 0
                  storeOplogTimestamp(shard, status.timestamp)
                }
              }
            }
          } catch {
            case e: Throwable => {
              if(MgClientUtils.isInterrupted(e))
                interrupted = true
              else
                logger.error("[" + syncKey + "] oplog process", e)
            }
          }
        }
      }

      if(bulkProcessor != null)
        bulkProcessor.close()

      logger.debug("[" + syncKey + "] stop main thread")
    }
  }

  private var mainThread: Thread = null
  private val oplogThreads = new ArrayList[Thread]()

  override def start(): Unit = {
    if(mainThread != null)
      throw new IllegalStateException("Sync already started")
    mainThread = new Thread(new MainThread())
    mainThread.setDaemon(true)
    mainThread.start()
  }

  override def stop(): Unit = {
    try {
      if(oplogThreads.size() > 0) {
        oplogThreads.forEach(thread =>thread.interrupt())
        oplogThreads.clear()
      }
      if(mainThread != null) {
        mainThread.interrupt()
        mainThread = null
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
