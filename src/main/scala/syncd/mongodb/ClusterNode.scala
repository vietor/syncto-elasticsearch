package syncd.mongodb

import java.lang.Integer
import java.util.{ArrayList}
import scala.util.Using

import org.bson._
import org.bson.types._
import com.mongodb._

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
    Using.resource(oplogCollection.find().sort(new BasicDBObject("$natural", -1)).limit(1).iterator()) {
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
        Using.resource(clusterClient.getDatabase(MgConstants.CONFIG_DATABASE).getCollection(MgConstants.SHARD_COLLECTION, classOf[BasicDBObject]).find().iterator()) {
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

  def getClient(): MongoClient = {
    clusterClient
  }

  def getServerNode(): MgServerNode = {
    serverNode
  }
}
