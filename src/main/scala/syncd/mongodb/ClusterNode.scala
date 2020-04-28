package syncd.mongodb

import java.lang.Integer
import java.util.ArrayList
import scala.collection.JavaConverters._

import org.bson._
import org.bson.types._
import com.mongodb._
import com.mongodb.client.MongoCollection
import syncd.utils.Managed._

class MgClusterNode(cluster: MgCluster) {

  private val clusterClient = MgClientUtils.createClient(cluster.servers, cluster.auth)

  private def isMongos(): Boolean = {
    clusterClient.getDatabase(MgConstants.ADMIN_DATABASE)
      .runCommand(new BasicDBObject() {
        put("serverStatus", 1: Integer)
        put("asserts", 0: Integer)
        put("backgroundFlushing", 0: Integer)
        put("connections", 0: Integer)
        put("cursors", 0: Integer)
        put("dur", 0: Integer)
        put("extra_info", 0: Integer)
        put("globalLock", 0: Integer)
        put("indexCounters", 0: Integer)
        put("locks", 0: Integer)
        put("metrics", 0: Integer)
        put("network", 0: Integer)
        put("opcounters", 0: Integer)
        put("opcountersRepl", 0: Integer)
        put("recordStats", 0: Integer)
        put("repl", 0: Integer)
      }, ReadPreference.primary())
      .get("process").toString().toLowerCase().contains("mongos")
  }


  private def getOplogCollectionName(client: MongoClient): String = {
    val collections = new ArrayList[String]()
    client.getDatabase(MgConstants.LOCAL_DATABASE).listCollectionNames().into(collections);
    if(collections.contains(MgConstants.OPLOG_COLLECTION1))
      MgConstants.OPLOG_COLLECTION1
    else if(collections.contains(MgConstants.OPLOG_COLLECTION2))
      MgConstants.OPLOG_COLLECTION2
    else
      throw new IllegalStateException("Cannot find " + MgConstants.OPLOG_COLLECTION1 + " or " + MgConstants.OPLOG_COLLECTION2 + " collection.")
  }

  private def getMgTimestamp(client: MongoClient, oplogName: String): MgTimestamp = {
    val oplogCollection = client.getDatabase(MgConstants.LOCAL_DATABASE).getCollection(oplogName, classOf[BasicDBObject])
    using(oplogCollection.find().sort(new BasicDBObject("$natural", -1)).limit(1).iterator()) {
      cursor => {
        if(!cursor.hasNext())
          MgTimestamp(0, 0)
        else
          MgClientUtils.convertTimestamp(cursor.next().get("ts").asInstanceOf[BSONTimestamp])
      }
    }
  }

  private val serverNode = {
    if(isMongos()) {
      MgServerNode(true, new ArrayList[MgShardNode]() {
        using(clusterClient.getDatabase(MgConstants.CONFIG_DATABASE).getCollection(MgConstants.SHARD_COLLECTION, classOf[BasicDBObject]).find().iterator()) {
          cursor => {
            while(cursor.hasNext()) {
              val item = cursor.next()
              val address = {
                val host = {
                  val host = item.get("host").toString
                  if (!host.contains("/"))
                    host
                  else
                    host.substring(host.indexOf("/") + 1);
                }
                new ArrayList[MgServer]() {
                  for (server <- host.split(",")){
                    add(MgClientUtils.convertAddress(server))
                  }
                }
              }
              val shardClient = MgClientUtils.createClient(address, cluster.auth)
              val oplogName = getOplogCollectionName(shardClient);
              add(MgShardNode(
                {
                  val id = item.get("_id").toString
                  if(!id.contains(","))
                    id
                  else
                    id.substring(0, id.indexOf(","));
                },
                address,
                getMgTimestamp(shardClient, oplogName),
                oplogName
              ))
            }
          }
        }
      })
    }
    else {
      MgServerNode(false, {
        val oplogName = getOplogCollectionName(clusterClient);
        new ArrayList[MgShardNode](){
          add(MgShardNode(
            "unshared",
            cluster.servers,
            getMgTimestamp(clusterClient, oplogName),
            oplogName
          ))
        }
      })
    }
  }

  private def getClient(): MongoClient = {
    clusterClient
  }

  def getServerNode(): MgServerNode = {
    serverNode
  }
}

object MgClusterNode {

  case class ImportContext(
    client: MongoClient,
    config: MgConfig
  )

  private def filterDBObject(config: MgConfig, o: DBObject): DBObject = {
    val resp = {
      if(config.include_fields == null || config.include_fields.size() < 1)
        o
      else {
        val out = new BasicDBObject() {
          for(k <- o.keySet().asScala) {
            if(config.include_fields contains k)
              put(k, o.get(k))
          }
        }
        out
      }
    }
    if(resp.containsField("_id"))
      resp.removeField("_id")
    resp
  }

  private def hasIncludeFields(config: MgConfig, o: DBObject): Boolean = {
    if(config.include_fields == null || config.include_fields.size() < 1)
      true
    else {
      o.keySet().asScala.exists(k => {
        if(k == "_id")
          false
        else {
          val pos = k.indexOf(".")
          if(pos == -1)
            config.include_fields contains k
          else
            config.include_fields contains k.substring(0, pos)
        }
      })
    }
  }

  private def filterUpdateDBObject(config: MgConfig, o: DBObject): (BasicDBObject, BasicDBObject) = {
    var parted = false
    val fields = new ArrayList[String]
    val resp = {
      new BasicDBObject() {
        if(config.include_fields == null || config.include_fields.size() < 1) {
          for(k <- o.keySet().asScala) {
            if(k != "_id") {
              val pos = k.indexOf(".")
              if(pos == -1) {
                fields.add(k)
                put(k, o.get(k))
              }
              else {
                val sk = k.substring(0, pos)
                parted = true
                fields.add(sk)
              }
            }
          }
        }
        else {
          for(k <- o.keySet().asScala) {
            if(k != "_id") {
              val pos = k.indexOf(".")
              if(pos == -1) {
                if(config.include_fields contains k) {
                  fields.add(k)
                  put(k, o.get(k))
                }
              } else {
                val sk = k.substring(0, pos)
                if(config.include_fields contains sk) {
                  parted = true
                  fields.add(sk)
                }
              }
            }
          }
        }
      }
    }
    ({
      if(!parted)
        null
      else
        new BasicDBObject() {
          fields.forEach(k =>
            append(k, 1)
          )
        }
    }, resp)
  }

  def createImportContext(cluster: MgClusterNode, config: MgConfig): ImportContext = {
    ImportContext(cluster.getClient(), config)
  }

  def importCollection(context: ImportContext, retry:()=> Int, iterate: (MgRecord) => Int) {
    val client = context.client
    val config = context.config

    var lastId: Object = null
    var inProgress: Boolean = true
    val collection = client.getDatabase(config.db).getCollection(config.collection, classOf[BasicDBObject])
    while(inProgress) {
      try {
        using(collection.find({
          if(lastId != null)
            new BasicDBObject("_id", new BasicDBObject("$gt", lastId))
          else
            new BasicDBObject()
        }).projection({
          if(config.include_fields == null || config.include_fields.size() < 1)
            new BasicDBObject()
          else {
            new BasicDBObject() {
              put("_id", 1: Integer)
              config.include_fields.forEach(key =>
                put(key, 1: Integer)
              )
            }
          }
        }).sort(new BasicDBObject("_id", 1)).iterator()) {
          cursor => {
            while (inProgress && cursor.hasNext()) {
              val row =  cursor.next();
              lastId = row.get("_id")
              inProgress = iterate(MgRecord(row.get("_id"), filterDBObject(config, row))) == MgConstants.RECORD_NEXT
            }
            inProgress = false;
          }
        }
      }
      catch {
        case e :Throwable => {
          if(!MgClientUtils.isRetrySafety(e))
            throw e
          val delayMS = retry()
          if(delayMS < 1)
            inProgress = false
          else
            Thread.sleep(delayMS)
        }
      }
    }
  }

  case class OplogContext(
    config: MgConfig,
    client: MongoClient,
    collection: MongoCollection[BasicDBObject],
    projection: BasicDBObject,
    oplogName: String
  )

  def createOplogContext(cluster: MgClusterNode, shard: MgShardNode, config: MgConfig): OplogContext = {
    OplogContext(
      config,
      MgClientUtils.createClient(shard.replicas, config.cluster.auth),
      cluster.getClient().getDatabase(config.db).getCollection(config.collection, classOf[BasicDBObject]),
      {
        if(config.include_fields == null || config.include_fields.size() < 1)
          null
        else {
          new BasicDBObject() {
            config.include_fields.forEach(k =>
              append(k, 1)
            )
          }
        }
      },
      shard.oplogName
    )
  }

  def syncCollectionOplog(context: OplogContext, timestamp: MgTimestamp, iterate: (MgOpRecord) => Int) {
    val client = context.client
    val config = context.config
    val collection = context.collection
    val projection = context.projection

    def getDBObject(row: DBObject, k: String): DBObject = {
      row.get(k).asInstanceOf[DBObject]
    }

    def mergeDBObject(a: DBObject, b: DBObject): DBObject = {
      val out = new BasicDBObject() {
        for(k <- a.keySet().asScala) {
          put(k, a.get(k))
        }
        for(k <- b.keySet().asScala) {
          put(k, b.get(k))
        }
      }
      out
    }

    def isInternalMoving(row: DBObject): Boolean = {
      val KEY = "fromMigrate"
      row.containsField(KEY) && row.get(KEY).asInstanceOf[Boolean]
    }

    def fetchOriginDBObject(timestamp: MgTimestamp, op: Int, _id: Object, fields: BasicDBObject): MgOpRecord = {
      using(collection.find(new BasicDBObject("_id", _id)).projection(fields).iterator()) {
        cursor => {
          if(!cursor.hasNext())
            MgOpRecord(timestamp, MgConstants.OP_IGNORE)
          else
            MgOpRecord(timestamp, op, _id, filterDBObject(config, cursor.next()))
        }
      }
    }

    val ns = config.db + "." + config.collection
    val oplogrs = client.getDatabase(MgConstants.LOCAL_DATABASE).getCollection(context.oplogName, classOf[BasicDBObject])
    using(oplogrs.find(new BasicDBObject("ts", new BasicDBObject("$gt", MgClientUtils.convertTimestamp(timestamp)))).cursorType(CursorType.TailableAwait).noCursorTimeout(true).oplogReplay(true).iterator()) {
      cursor => {
        var inProgress: Boolean = true
        while(inProgress && cursor.hasNext()) {
          val row = cursor.next()
          val current = MgClientUtils.convertTimestamp(row.get("ts").asInstanceOf[BSONTimestamp])
          val opRecord = {
            if(row.get("ns") != ns)
              MgOpRecord(current, MgConstants.OP_IGNORE)
            else if(isInternalMoving(row))
              MgOpRecord(current, MgConstants.OP_IGNORE)
            else
              row.get("op").toString match {
                case "i" => {
                  val doc = getDBObject(row, "o")
                  MgOpRecord(current, MgConstants.OP_CREATE, doc.get("_id"), filterDBObject(config, doc))
                }
                case "u" => {
                  val _id = getDBObject(row, "o2").get("_id")
                  val update = getDBObject(row, "o")
                  if(!update.keySet().asScala.exists(x => x.substring(0, 1) == "$"))
                    MgOpRecord(current, MgConstants.OP_RECREATE, _id, filterDBObject(config, update))
                  else if(update.containsField("$unset") && hasIncludeFields(config, getDBObject(update, "$unset")))
                    fetchOriginDBObject(current, MgConstants.OP_RECREATE, _id, projection)
                  else if(!update.containsField("$set"))
                    MgOpRecord(current, MgConstants.OP_IGNORE)
                  else{
                    val (parted, doc) = filterUpdateDBObject(config, getDBObject(update, "$set"))
                    if(parted != null)
                      fetchOriginDBObject(current, MgConstants.OP_UPDATE, _id, parted)
                    else {
                      if(doc.keySet().size() < 1)
                        MgOpRecord(current, MgConstants.OP_IGNORE)
                      else
                        MgOpRecord(current, MgConstants.OP_UPDATE, _id, doc)
                    }
                  }
                }
                case "d" => {
                  val _id = getDBObject(row, "o").get("_id")
                  MgOpRecord(current, MgConstants.OP_DELETE, _id)
                }
                case _ => {
                  MgOpRecord(current, MgConstants.OP_IGNORE)
                }
              }
          }
          inProgress = iterate(opRecord) == MgConstants.RECORD_NEXT
        }
      }
    }
  }
}
