package syncd.elasticsearch

import java.util.List
import java.util.HashMap

import org.elasticsearch.common.unit._
import org.elasticsearch.action.bulk._
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest

import syncd.utils._

case class EsBulkParameters(
  actions: Int = 1000,
  bytesOnMB: Int = 5,
  flushIntervalOnMillis: Int = 10,
  itemsErrorWatcher: (Throwable) => Unit = null,
  globalErrorWatcher: (Throwable) => Unit = null
)

trait EsBulkSyncTimer {
  def getSyncTime(): Long
}

class EsBulkProcessor(index: String, timer: EsBulkSyncTimer, processor: BulkProcessor) {

  private var lastSyncTime: Long = timer.getSyncTime()

  def resetSync(): Unit = {
    lastSyncTime = timer.getSyncTime()
  }

  def detectSync(): Boolean = {
    val time = timer.getSyncTime()
    var changed = time != lastSyncTime
    if(changed) {
      lastSyncTime = time
    }
    changed
  }

  def index(id: String, doc: String): Unit = {
    processor.add(new IndexRequest(index, EsConstants.DEFAULT_TYPE, id).source(doc, XContentType.JSON))
  }

  def update(id: String, doc: String): Unit = {
    processor.add(new UpdateRequest(index, EsConstants.DEFAULT_TYPE,  id).doc(doc, XContentType.JSON))
  }

  def delete(id: String): Unit = {
    processor.add(new DeleteRequest(index, EsConstants.DEFAULT_TYPE, id))
  }

  def flush(): Unit = {
    processor.flush()
  }

  def close(): Unit = {
    try {
      processor.close()
    } catch {
      case _: Throwable => {}
    }
  }
}

class EsClusterNode(cluster: EsCluster) {
  val clusterClient  = EsClientUtils.createClient(cluster.servers)

  def exists(index: String): Boolean = {
    val request = new GetIndexRequest()
    request.indices(index);
    clusterClient.indices().exists(request, RequestOptions.DEFAULT);
  }

  def create(index: String, settings: String = null, mapping: String = null): Unit = {
    val request = new CreateIndexRequest(index)
    if(settings != null && !settings.isEmpty())
      request.settings(settings, XContentType.JSON)
    if(mapping != null && !mapping.isEmpty())
      request.mapping(EsConstants.DEFAULT_TYPE, mapping, XContentType.JSON)
    clusterClient.indices().create(request, RequestOptions.DEFAULT)
  }

  def modifyMapping(index: String, mapping: String): Unit = {
    val request = new PutMappingRequest(index)
    request.`type`(EsConstants.DEFAULT_TYPE)
    request.source(mapping, XContentType.JSON)
    clusterClient.indices().putMapping(request, RequestOptions.DEFAULT)
  }

  def directIndex(index: String, id: String, doc: String): Unit = {
    clusterClient.index(new IndexRequest(index, EsConstants.DEFAULT_TYPE, id).source(doc, XContentType.JSON), RequestOptions.DEFAULT)
  }

  def directUpdate(index: String, id: String, doc: String): Unit = {
    clusterClient.update(new UpdateRequest(index, EsConstants.DEFAULT_TYPE, id).doc(doc, XContentType.JSON), RequestOptions.DEFAULT)
  }

  def directDelete(index: String, id: String): Unit = {
    clusterClient.delete(new DeleteRequest(index, EsConstants.DEFAULT_TYPE, id), RequestOptions.DEFAULT)
  }

  def bulkRequest(index: String, requests: List[EsRequest]): BulkResponse = {
    val request = new BulkRequest()
    requests.forEach(row => {
      row.op match {
        case EsConstants.OP_INDEX =>
          request.add(new IndexRequest(index, EsConstants.DEFAULT_TYPE, row.id).source(row.doc, XContentType.JSON))
        case EsConstants.OP_UPDATE =>
          request.add(new UpdateRequest(index, EsConstants.DEFAULT_TYPE, row.id).doc(row.doc, XContentType.JSON).retryOnConflict(3).docAsUpsert(true))
        case EsConstants.OP_DELETE =>
          request.add(new DeleteRequest(index, EsConstants.DEFAULT_TYPE, row.id))
      }
    })
    clusterClient.bulk(request, RequestOptions.DEFAULT)
  }

  def createBulkProcessor(index: String, parameters: EsBulkParameters): EsBulkProcessor = {
    abstract class EsBulkProcessorListener extends EsBulkSyncTimer with BulkProcessor.Listener{}

    val listener = new EsBulkProcessorListener() {
      private var syncTime: Long = System.currentTimeMillis

      override def getSyncTime(): Long = {
        syncTime
      }
      override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
      }
      override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
        syncTime = System.currentTimeMillis

        if(parameters.itemsErrorWatcher != null) {
          if(response.hasFailures()) {
            for(item <- response.getItems()) {
              if(item.isFailed()) {
                parameters.itemsErrorWatcher(item.getFailure().getCause())
              }
            }
          }
        }
      }
      override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
        if(parameters.globalErrorWatcher != null)
          parameters.globalErrorWatcher(failure)
      }
    }
    new EsBulkProcessor(index, listener, {
      BulkProcessor.builder((request, bulkListener) => clusterClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener)
        .setBulkActions(parameters.actions)
        .setBulkSize(new ByteSizeValue(parameters.bytesOnMB, ByteSizeUnit.MB))
        .setFlushInterval(TimeValue.timeValueMillis(parameters.flushIntervalOnMillis))
        .setConcurrentRequests(Runtime.getRuntime().availableProcessors())
        .build()
    })
  }
}
