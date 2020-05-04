package syncd.mongodb

import java.lang.Integer
import java.util.{ArrayList}
import scala.util.Using
import scala.jdk.CollectionConverters._

import org.bson._
import org.bson.types._
import com.mongodb._
import com.mongodb.client.MongoCollection

import syncd.engine.SyncdConfig

object MgTransmission {

  private object DBOUtil {
    def child(o: DBObject, k: String): DBObject = {
      o.get(k).asInstanceOf[DBObject]
    }

    def pick(o: DBObject, include_fields: ArrayList[String]): DBObject = {
      val resp = {
        if(include_fields == null || include_fields.size() < 1)
          o
        else {
          new BasicDBObject() {
            for(k <- o.keySet().asScala) {
              if(include_fields contains k)
                put(k, o.get(k))
            }
          }
        }
      }
      if(resp.containsField("_id"))
        resp.removeField("_id")
      resp
    }

    def includes(o: DBObject, include_fields: ArrayList[String]): Boolean = {
      if(include_fields == null || include_fields.size() < 1)
        true
      else {
        o.keySet().asScala.exists(k => {
          if(k == "_id")
            false
          else {
            val pos = k.indexOf(".")
            if(pos == -1)
              include_fields contains k
            else
              include_fields contains k.substring(0, pos)
          }
        })
      }
    }

    def pickForUpdate(o: DBObject, include_fields: ArrayList[String]): (BasicDBObject, BasicDBObject) = {
      var parted = false
      val exist_fields = new ArrayList[String]
      val resp = {
        new BasicDBObject() {
          if(include_fields == null || include_fields.size() < 1) {
            for(k <- o.keySet().asScala) {
              if(k != "_id") {
                val pos = k.indexOf(".")
                if(pos == -1) {
                  put(k, o.get(k))
                  exist_fields.add(k)
                }
                else {
                  parted = true
                  exist_fields.add(k.substring(0, pos))
                }
              }
            }
          }
          else {
            for(k <- o.keySet().asScala) {
              if(k != "_id") {
                val pos = k.indexOf(".")
                if(pos == -1) {
                  if(include_fields contains k) {
                    put(k, o.get(k))
                    exist_fields.add(k)
                  }
                } else {
                  val sk = k.substring(0, pos)
                  if(include_fields contains sk) {
                    parted = true
                    exist_fields.add(sk)
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
            exist_fields.forEach(k =>
              append(k, 1)
            )
          }
      }, resp)
    }
  }

  case class ImportContext(
    config: MgConfig,
    queryFields: BasicDBObject,
    querySort: BasicDBObject,
    masterCollection: MongoCollection[BasicDBObject]
  )

  case class OplogContext(
    config: MgConfig,
    shard: MgShardNode,
    fetchFields: BasicDBObject,
    masterCollection: MongoCollection[BasicDBObject],
    oplogCollection: MongoCollection[BasicDBObject]
  )

  def createImportContext(cluster: MgClusterNode, config: MgConfig): ImportContext = {
    ImportContext(
      config,
      {
        if(config.include_fields == null || config.include_fields.size() < 1)
          new BasicDBObject()
        else {
          new BasicDBObject() {
            put("_id", 1: Integer)
            config.include_fields.forEach(key => put(key, 1: Integer))
          }
        }
      },
      new BasicDBObject("_id", 1),
      cluster.getClient().getDatabase(config.db).getCollection(config.collection, classOf[BasicDBObject]),
    )
  }

  def importCollection(syncdConfig: SyncdConfig, context: ImportContext, iterate: (MgRecord) => Unit): Unit = {
    val config = context.config

    var lastId: Object = null
    var inProgress: Boolean = true
    while(inProgress) {
      try {
        Using.resource(context.masterCollection.find({
          if(lastId != null)
            new BasicDBObject("_id", new BasicDBObject("$gt", lastId))
          else
            new BasicDBObject()
        }).projection(context.queryFields).sort(context.querySort).iterator()) {
          cursor => {
            while (cursor.hasNext()) {
              val row =  cursor.next();
              lastId = row.get("_id")
              iterate(MgRecord(row.get("_id"), DBOUtil.pick(row, config.include_fields)))
            }
            inProgress = false;
          }
        }
      }
      catch {
        case e :Throwable => {
          if(!MgClientUtils.isRetrySafety(e))
            throw e
          else
            Thread.sleep(syncdConfig.intervalRetryMS)
        }
      }
    }
  }

  def createOplogContext(cluster: MgClusterNode, shard: MgShardNode, config: MgConfig): OplogContext = {
    val shardClient = MgClientUtils.createClient(shard.replicas, config.cluster.auth)
    OplogContext(
      config,
      shard,
      {
        if(config.include_fields == null || config.include_fields.size() < 1)
          null
        else {
          new BasicDBObject() {
            config.include_fields.forEach(k => append(k, 1))
          }
        }
      },
      cluster.getClient().getDatabase(config.db).getCollection(config.collection, classOf[BasicDBObject]),
      shardClient.getDatabase(MgConstants.LOCAL_DATABASE).getCollection(shard.oplogName, classOf[BasicDBObject])
    )
  }

  def syncCollectionOplog(context: OplogContext, timestamp: MgTimestamp, iterate: (MgOpRecord) => Unit): Unit = {
    val config = context.config
    val shard = context.shard

    def isInternalMoving(row: DBObject): Boolean = {
      val KEY = "fromMigrate"
      row.containsField(KEY) && row.get(KEY).asInstanceOf[Boolean]
    }

    def fetchFromMaster(timestamp: MgTimestamp, op: Int, _id: Object, fields: BasicDBObject): MgOpRecord = {
      Using.resource(context.masterCollection.find(new BasicDBObject("_id", _id)).projection(fields).iterator()) {
        cursor => {
          if(!cursor.hasNext())
            MgOpRecord(shard.name, timestamp, MgConstants.OP_IGNORE)
          else
            MgOpRecord(shard.name, timestamp, op, _id, DBOUtil.pick(cursor.next(), config.include_fields))
        }
      }
    }

    Using.resource(context.oplogCollection.find(new BasicDBObject("ts", new BasicDBObject("$gt", MgClientUtils.convertTimestamp(timestamp)))).cursorType(CursorType.TailableAwait).noCursorTimeout(true).oplogReplay(true).iterator()) {
      cursor => {
        val ns = config.db + "." + config.collection
        while(cursor.hasNext()) {
          val row = cursor.next()
          val current = MgClientUtils.convertTimestamp(row.get("ts").asInstanceOf[BSONTimestamp])
          val opRecord = {
            if(row.get("ns") != ns)
              MgOpRecord(shard.name, current, MgConstants.OP_IGNORE)
            else if(isInternalMoving(row))
              MgOpRecord(shard.name, current, MgConstants.OP_IGNORE)
            else
              row.get("op").toString match {
                case "i" => {
                  val doc = DBOUtil.child(row, "o")
                  MgOpRecord(shard.name, current, MgConstants.OP_INSERT, doc.get("_id"), DBOUtil.pick(doc, config.include_fields))
                }
                case "u" => {
                  val _id = DBOUtil.child(row, "o2").get("_id")
                  val update = DBOUtil.child(row, "o")
                  if(!update.keySet().asScala.exists(x => x.substring(0, 1) == "$"))
                    MgOpRecord(shard.name, current, MgConstants.OP_REINSERT, _id, DBOUtil.pick(update, config.include_fields))
                  else if(update.containsField("$unset") && DBOUtil.includes(DBOUtil.child(update, "$unset"), config.include_fields))
                    fetchFromMaster(current, MgConstants.OP_REINSERT, _id, context.fetchFields)
                  else if(!update.containsField("$set"))
                    MgOpRecord(shard.name, current, MgConstants.OP_IGNORE)
                  else{
                    val (parted, doc) = DBOUtil.pickForUpdate(DBOUtil.child(update, "$set"), config.include_fields)
                    if(parted != null)
                      fetchFromMaster(current, MgConstants.OP_UPDATE, _id, parted)
                    else {
                      if(doc.keySet().size() < 1)
                        MgOpRecord(shard.name, current, MgConstants.OP_IGNORE)
                      else
                        MgOpRecord(shard.name, current, MgConstants.OP_UPDATE, _id, doc)
                    }
                  }
                }
                case "d" => {
                  val _id = DBOUtil.child(row, "o").get("_id")
                  MgOpRecord(shard.name, current, MgConstants.OP_DELETE, _id)
                }
                case _ => {
                  MgOpRecord(shard.name, current, MgConstants.OP_IGNORE)
                }
              }
          }
          iterate(opRecord)
        }
      }
    }
  }
}
