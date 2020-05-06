package syncd.elasticsearch

import java.util.{ArrayList, HashMap}
import scala.jdk.CollectionConverters._

import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder

import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import org.elasticsearch.client.RestHighLevelClient

object EsClientUtils {

  class EsAuthCallback(auth: EsAuth) extends RestClientBuilder.HttpClientConfigCallback {
    private val credentialsProvider = {
      val provider = new BasicCredentialsProvider();
      provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth.username, auth.password))
      provider
    }

    override def customizeHttpClient(builder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
      builder.setDefaultCredentialsProvider(credentialsProvider)
    }
  }

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

  def createClient(servers: ArrayList[EsServer], auth: EsAuth): RestHighLevelClient = {
    val key = new ClientCacheKey(servers)
    if(caches.containsKey(key))
      caches.get(key)
    else {
      val hosts = servers.asScala.map((row) => new HttpHost(row.host, row.port, "http"))
      val builder = RestClient.builder(hosts.toSeq: _*)
      if(auth != null) {
        builder.setHttpClientConfigCallback(new EsAuthCallback(auth))
      }
      val client = new RestHighLevelClient(builder)
      caches.put(key, client)
      client
    }
  }
}
