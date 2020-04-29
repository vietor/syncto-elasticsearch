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
      JsonUtil.readValue(ktvtStore.get("oplog", "master", JsonUtil.writeValueAsString(opTimestamp)), classOf[MyTimestamp])
    } catch {
      case _: Throwable => opTimestamp
    }
  }

  private def storeOplogTimestamp(opTimestamp: MyTimestamp): Unit = {
    ktvtStore.put("oplog", "master", JsonUtil.writeValueAsString(opTimestamp))
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

        bulkProcessor = esCluster.createBulkProcessor(
          esConfig.index,
          EsBulkParameters(
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
        }

        setStatusStep("OPLOG")
        storeOplogTimestamp(myCluster.getServerNode().timestamp)

      } catch {
        case e: Throwable => {
          setStatus(Status.START_FAILED)
          if(!MyClientUtils.isInterrupted(e))
            logger.error("[" + syncKey + "] MySQL to ElasticSearch Starting", e)
        }
      }
    }
  }

  private var mainThread: Thread = null

  override def start(): Unit = {
    if(mainThread != null)
      throw new IllegalStateException("Sync already started")

    new MainThread().run()
  }

  override def stop(): Unit = {
    try {
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
