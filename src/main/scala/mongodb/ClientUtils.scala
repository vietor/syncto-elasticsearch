package mongodbsync.mongodb

import java.util.HashMap
import java.util.ArrayList

import com.mongodb._
import org.bson.types._

object MgClientUtils {

  private class ClientCacheKey(servers: ArrayList[MgServer]) {
    override def hashCode(): Int = {
      var result = 1
      result = 31 * result + servers.hashCode()
      result
    }

    override def equals(obj:  Any): Boolean = {
      obj match {
        case that: ClientCacheKey =>
          hashCode() == that.hashCode()
        case _ => false
      }
    }
  }

  private val caches = new HashMap[ClientCacheKey, MongoClient]()

  def createClient(servers: ArrayList[MgServer], auth: MgAuth): MongoClient = {
    val key = new ClientCacheKey(servers)
    if(caches.containsKey(key))
      caches.get(key)
    else {
      val client = {
        val seeds = new ArrayList[ServerAddress](){
          servers.forEach(row => add(convertAddress(row)))
        }
        val options = new MongoClientOptions.Builder().build()
        if(auth == null)
          new MongoClient(seeds, options)
        else
          new MongoClient(seeds, MongoCredential.createCredential(auth.username,  auth.database, auth.password.toCharArray()), options)
      }
      caches.put(key, client)
      client
    }
  }

  def convertAddress(origin: String): MgServer = {
    convertAddress(new ServerAddress(origin))
  }

  def convertAddress(origin: ServerAddress): MgServer = {
    MgServer(origin.getHost(), origin.getPort())
  }

  def convertAddress(origin: MgServer): ServerAddress = {
    new ServerAddress(origin.host, origin.port)
  }

  def convertTimestamp(origin: BSONTimestamp): MgTimestamp = {
    MgTimestamp(origin.getTime(), origin.getInc())
  }

  def convertTimestamp(origin: MgTimestamp): BSONTimestamp = {
    new BSONTimestamp(origin.time.toInt, origin.seq.toInt)
  }

  def isRetrySafety(e: Throwable): Boolean = {
    e.isInstanceOf[MongoSocketException] || e.isInstanceOf[MongoTimeoutException] || e.isInstanceOf[MongoCursorNotFoundException]
  }

  def isInterrupted(e: Throwable): Boolean = {
    e.isInstanceOf[InterruptedException] ||  e.isInstanceOf[MongoInterruptedException]
  }
}
