package syncd.elasticsearch

import java.util.{ArrayList, HashMap}
import scala.jdk.CollectionConverters._

import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient

object EsClientUtils {
  private class ClientCacheKey(servers: ArrayList[EsServer]) {
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

  private val caches = new HashMap[ClientCacheKey, RestHighLevelClient]()

  def createClient(servers: ArrayList[EsServer]): RestHighLevelClient = {
    val key = new ClientCacheKey(servers)
    if(caches.containsKey(key))
      caches.get(key)
    else {
      var hosts = servers.asScala.map((row) => new HttpHost(row.host, row.port, "http"))
      val client = new RestHighLevelClient(RestClient.builder(hosts.toSeq: _*))
      caches.put(key, client)
      client
    }
  }
}
