package mongodbsync.engine

import java.util.{Map, HashMap}
import java.util.ArrayList
import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.Handler
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.server.handler.HandlerCollection
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import mongodbsync.utils._
import mongodbsync.mongodb._
import mongodbsync.elasticsearch._

class WebSynchronManager(port: Int, syncConfig: SyncConfig, ktvtDB: KtVtDatabase) {
  private val VERSION = "2.0"
  private val tsForUptime = SomeUtil.getTimestamp()
  private val logger = LoggerFactory.getLogger(getClass().getName())

  private case class Worker(
    key: String,
    meta: String,
    sync: AbstractSync
  )

  private val lock = new Object()
  private val locales = ktvtDB.getLocalCollection()
  private val workers = new HashMap[String, Worker]() {
    locales.getKeys().forEach(key => {
      val fields = locales.getAll(key)
      if(fields != null) {
        try {
          val body =  JsonUtil.readTree(fields.get("metadata"))
          val sync = new ToElasticsearchSync(syncConfig, key, JsonUtil.convertValue(body.get("mongodb"), classOf[MgConfig]), JsonUtil.convertValue(body.get("elasticsearch"), classOf[EsConfig]), ktvtDB.getCollection(key))
          put(key, Worker(
            key,
            fields.get("metadata"),
            sync
          ))
        } catch {
          case e: Throwable => {
            logger.error("Sync " + key, e)
          }
        }
      }
    })
  }

  private def sendResponse[T](response: HttpServletResponse, responseData: T) {
    response.setContentType("application/json; charset=utf-8")
    response.setStatus(HttpServletResponse.SC_OK)
    response.getWriter.print(JsonUtil.writeValueAsPrettyString(responseData))
  }

  val statusHandler = new AbstractHandler() {

    private def dumpWorkerStatus(worker: Worker, summary: Boolean = false):HashMap[String, Any] = {
      new HashMap[String, Any](){
        put("key", worker.key)
        put("status", worker.sync.getStatusText())
        if(summary)
          put("summary", worker.sync.dumpSummary())
      }
    }

    override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) {
      if(target == "/_status" && request.getMethod() == "GET") {
        baseRequest.setHandled(true)

        val responseData = {
          try {
            val timestamp = SomeUtil.getTimestamp()
            new HashMap[String, Any]() {
              put("version", VERSION)
              put("timestamp", timestamp)
              put("uptime", timestamp - tsForUptime)
              put("workers", new ArrayList[Any]() {
                lock.synchronized {
                  for((key, worker) <- workers.asScala) {
                    add(dumpWorkerStatus(worker))
                  }
                }
              })
              put("config", new HashMap[String, Int]() {
                put("batch_queue_size", syncConfig.batchQueueSize)
                put("batch_size_mb", syncConfig.batchSizeMB)
                put("interval_oplog_ms", syncConfig.intervalOplogMS)
                put("interval_retry_ms", syncConfig.intervalRetryMS)
              })
            }
          } catch {
            case e: Throwable => {
              new HashMap[String, Any]() {
                put("error", e.getMessage())
              }
            }
          }
        }

        sendResponse(response, responseData)
      } else if(target.matches("^\\/_worker\\/[^\\/]+\\/_status") && request.getMethod() == "GET") {
        baseRequest.setHandled(true)

        val responseData = {
          try {
            lock.synchronized {
              val key = target.substring(9, target.indexOf("/_status")).toLowerCase()
              if(!workers.containsKey(key))
                throw new IllegalStateException("Worker not exists")

              dumpWorkerStatus(workers.get(key), true)
            }
          } catch {
            case e: Throwable => {
              new HashMap[String, Any]() {
                put("error", e.getMessage())
              }
            }
          }
        }

        sendResponse(response, responseData)
      }
    }
  }

  val createHandler = new AbstractHandler() {
    override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) {
      if(target.matches("^\\/_worker\\/[^\\/]+\\/_meta$") && request.getMethod() == "GET") {
        baseRequest.setHandled(true)

        val responseData = {
          try {
            lock.synchronized {
              val key = target.substring(9, target.indexOf("/_meta")).toLowerCase()
              if(!workers.containsKey(key))
                throw new IllegalStateException("Worker not exists")

              JsonUtil.readTree(workers.get(key).meta)
            }
          } catch {
            case e: Throwable => {
              new HashMap[String, Any]() {
                put("error", e.getMessage())
              }
            }
          }
        }

        sendResponse(response, responseData)
      } else if(target.matches("^\\/_worker\\/[^\\/]+\\/_meta$") && request.getMethod() == "PUT") {
        baseRequest.setHandled(true)

        val responseData = {
          try {
            lock.synchronized {
              val key = target.substring(9, target.indexOf("/_meta")).toLowerCase()
              if(workers.containsKey(key))
                throw new IllegalStateException("Worker already exists")

              val metadata = {
                var line: String = null
                val sb = new StringBuffer()
                val reader = request.getReader()
                do {
                  line = reader.readLine()
                  if(line != null)
                    sb.append(line)
                } while(line != null)

                sb.toString()
              }

              val body = JsonUtil.readTree(metadata)
              if(!body.has("mongodb"))
                throw new IllegalStateException("Metadata no field: mongodb")
              if(!body.has("elasticsearch"))
                throw new IllegalStateException("Metadata no field: elasticsearch")

              val sync = new ToElasticsearchSync(syncConfig, key, JsonUtil.convertValue(body.get("mongodb"), classOf[MgConfig]), JsonUtil.convertValue(body.get("elasticsearch"), classOf[EsConfig]), ktvtDB.getCollection(key))
              locales.putAll(key, new HashMap[String, String]() {
                put("metadata", metadata)
                put("timestamp", SomeUtil.getTimestamp().toString)
              })
              lock.synchronized {
                workers.put(key, Worker(
                  key,
                  metadata,
                  sync
                ))
                sync.start()
              }
              logger.info("Create worker {}", key)
              new HashMap[String, Any]() {
                put("ok", 1)
              }
            }
          } catch {
            case e: Throwable => {
              new HashMap[String, Any]() {
                put("error", e.getMessage())
              }
            }
          }
        }

        sendResponse(response, responseData)
      }
    }
  }

  val startHandler = new AbstractHandler() {
    override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) {
      if(target.matches("^\\/_worker\\/[^\\/]+\\/start") && request.getMethod() == "POST") {
        baseRequest.setHandled(true)

        val responseData = {
          try {
            lock.synchronized {
              val key = target.substring(9, target.indexOf("/start")).toLowerCase()
              if(!workers.containsKey(key))
                throw new IllegalStateException("Worker not exists")

              val worker = workers.get(key)
              worker.sync.start()

              logger.info("Start worker {}", key)
              new HashMap[String, Any]() {
                put("ok", 1)
              }
            }
          } catch {
            case e: Throwable => {
              new HashMap[String, Any]() {
                put("error", e.getMessage())
              }
            }
          }
        }

        sendResponse(response, responseData)
      }
    }
  }

  val stopHandler = new AbstractHandler() {
    override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) {
      if(target.matches("^\\/_worker\\/[^\\/]+\\/stop") && request.getMethod() == "POST") {
        baseRequest.setHandled(true)

        val responseData = {
          try {
            lock.synchronized {
              val key = target.substring(9, target.indexOf("/stop")).toLowerCase()
              if(!workers.containsKey(key))
                throw new IllegalStateException("Worker not exists")

              val worker = workers.get(key)
              worker.sync.stop()

              logger.info("Stop worker {}", key)
              new HashMap[String, Any]() {
                put("ok", 1)
              }
            }
          } catch {
            case e: Throwable => {
              new HashMap[String, Any]() {
                put("error", e.getMessage())
              }
            }
          }
        }

        sendResponse(response, responseData)
      }
    }
  }

  val deleteHandler = new AbstractHandler() {
    override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) {
      if(target.matches("^\\/_worker\\/[^/]+$") && request.getMethod() == "DELETE") {
        baseRequest.setHandled(true)

        val responseData = {
          try {
            lock.synchronized {
              val key = target.substring(9).toLowerCase()
              if(!workers.containsKey(key))
                throw new IllegalStateException("Worker not exists")

              val worker = workers.get(key)
              worker.sync.stop()
              workers.remove(worker.key)
              locales.removeAll(worker.key)
              ktvtDB.dropCollection(worker.key)

              logger.info("Delete worker {}", key)
              new HashMap[String, Any]() {
                put("ok", 1)
              }
            }
          } catch {
            case e: Throwable => {
              new HashMap[String, Any]() {
                put("error", e.getMessage())
              }
            }
          }
        }

        sendResponse(response, responseData)
      }
    }
  }

  val webServer = new Server(port)
  webServer.setHandler({
    val handlers = new ArrayList[Handler](){
      add(statusHandler)
      add(createHandler)
      add(startHandler)
      add(stopHandler)
      add(deleteHandler)
    }
    val collection = new HandlerCollection()
    collection.setHandlers(handlers.toArray.map(_.asInstanceOf[Handler]))
    collection
  });

  def start() {
    logger.info("Start rest api in port {}", port)

    lock.synchronized {
      for((key, worker) <- workers.asScala) {
        worker.sync.start()
        logger.info("Start worker {}", key)
      }
    }
    webServer.start()
  }

  def stop() {
    logger.info("Start rest api stop")

    webServer.stop()
    lock.synchronized {
      for((key, worker) <- workers.asScala) {
        worker.sync.stop()
        logger.info("Stop worker {}", key)
      }
    }
    ktvtDB.sync()
  }
}
