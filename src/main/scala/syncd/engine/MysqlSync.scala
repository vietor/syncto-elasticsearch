package syncd.engine

import java.util.{Map, HashMap}
import java.util.ArrayList
import scala.jdk.CollectionConverters._

import java.util.concurrent._
import org.slf4j.LoggerFactory
import org.elasticsearch.ElasticsearchStatusException

import syncd.utils._
import syncd.mysql._
import syncd.elasticsearch._

class MysqlSync(syncdConfig: SyncdConfig, syncKey: String,  myConfig: MyConfig, esConfig: EsConfig, ktvtStore: KtVtCollection) extends AbstractSync {
  private val logger = LoggerFactory.getLogger(getClass().getName())

  private def setStatusStep(step: String): Unit = {
    ktvtStore.put("status", "step", step)
  }

  private def readOplogTimestamp(opTimestamp: MyTimestamp): MyTimestamp = {
    try {
      JsonUtil.readValue(ktvtStore.get("oplog", "binlog", JsonUtil.writeValueAsString(opTimestamp)), classOf[MyTimestamp])
    } catch {
      case _: Throwable => opTimestamp
    }
  }

  private def storeOplogTimestamp(opTimestamp: MyTimestamp): Unit = {
    ktvtStore.put("oplog", "binlog", JsonUtil.writeValueAsString(opTimestamp))
  }

  private val oplogRecordQueue = new LinkedBlockingQueue[MyOpRecord](syncdConfig.batchQueueSize)

  private class OplogThread(cluster: MyClusterNode) extends Runnable {
    val context = MyTransmission.createOplogContext(cluster, myConfig)

    override def run(): Unit = {
      var opTimestamp = readOplogTimestamp(cluster.getServerNode().timestamp)

      try {
        while(true) {
          var sleepMS = syncdConfig.intervalOplogMS
          try {
            MyTransmission.syncCollectionOplog(context, opTimestamp, (record: MyOpRecord) => {
              opTimestamp = record.ts
              oplogRecordQueue.put(record)
            })
          } catch {
            case e: Throwable => {
              if(!MyClientUtils.isRetrySafety(e)) {
                if(MyClientUtils.isInterrupted(e))
                  throw e
                logger.error("[" + syncKey + "] Fetch oplog", e)
              }
              else
                sleepMS = syncdConfig.intervalRetryMS
            }
          }
          Thread.sleep(sleepMS)
        }
      } catch {
        case e: Throwable => {
          if(!MyClientUtils.isInterrupted(e))
            logger.error("[" + syncKey + "] Escape Oplog Thread", e)
        }
      }

      MyTransmission.releaseOplogContext(context)
    }
  }

  private class MainThread extends Runnable {
    override def run(): Unit = {
      var myCluster: MyClusterNode = null
      var esCluster: EsClusterNode = null
      var bulkProcessor: EsBulkProcessor = null

      try {
        setStatusStep("STARTUP")
        setStatus(Status.STARTING)

        var waitingStep = true
        while(waitingStep) {
          try {
            if(myCluster == null)
              myCluster = new MyClusterNode(myConfig)
            if(esCluster == null)
              esCluster = new EsClusterNode(esConfig.cluster)
            waitingStep = false
          }
          catch {
            case e: Throwable => {
              if(!MyClientUtils.isRetrySafety(e)) {
                setStatus(Status.START_FAILED)
                throw e
              }
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
            logger.error("[" + syncKey + "] Bulk items ", e)
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
          MyTransmission.importCollection(MyTransmission.createImportContext(myCluster, myConfig), ()=> {
            syncdConfig.intervalRetryMS
          }, (record: MyRecord) => {
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
          storeOplogTimestamp(myCluster.getServerNode().timestamp)
        }

        setStatusStep("OPLOG")
        val thread = new Thread(new OplogThread(myCluster))
        thread.setDaemon(true)
        oplogThreads.add(thread)
        oplogThreads.forEach(thread =>thread.start())

      } catch {
        case e: Throwable => {
          setStatus(Status.START_FAILED)
          if(!MyClientUtils.isInterrupted(e))
            logger.error("[" + syncKey + "] MySQL to ElasticSearch Starting", e)
        }
      }

      if(getStatus() == Status.RUNNING) {
        bulkProcessor.resetSync()

        var opTimestamp: MyTimestamp = null
        while(true) {
          try {
            val opRecord = oplogRecordQueue.poll(syncdConfig.intervalOplogMS, TimeUnit.MILLISECONDS)
            if(opRecord != null) {
              opTimestamp = opRecord.ts

              opRecord.op match {
                case MyConstants.OP_INSERT => {
                  opRecord.docs.forEach(record => {
                    if(record.id == null)
                      logger.warn("[" + syncKey + "] MySQL oplog " + opTimestamp + " INSERT lost pkey")
                    else
                      bulkProcessor.index(record.id.toString, JsonUtil.writeValueAsString(record.doc))
                  })
                }
                case MyConstants.OP_UPDATE => {
                  opRecord.docs.forEach(record => {
                    if(record.id == null)
                      logger.warn("[" + syncKey + "] MySQL oplog " + opTimestamp + " UPDATE lost pkey")
                    else
                      bulkProcessor.update(record.id.toString, JsonUtil.writeValueAsString(record.doc))
                  })
                }
                case MyConstants.OP_DELETE => {
                  opRecord.docs.forEach(record => {
                    if(record.id == null)
                      logger.warn("[" + syncKey + "] MySQL oplog " + opTimestamp + " DELETE lost pkey")
                    else
                      bulkProcessor.delete(record.id.toString)
                  })
                }
                case _ => {}
              }
            }
            if(opRecord == null)
              bulkProcessor.flush()

            if(bulkProcessor.detectSync()) {
              if(opTimestamp != null) {
                storeOplogTimestamp(opTimestamp)
              }
            }
          } catch {
            case e: Throwable => {
              if(!MyClientUtils.isInterrupted(e))
                logger.error("[" + syncKey + "] Poll MySQL oplog to Elasticsearch", e)
            }
          }
        }
      }

      if(bulkProcessor != null)
        bulkProcessor.close()

      oplogThreads.forEach(thread => thread.interrupt())
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
