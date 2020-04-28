package syncd.utils

import java.util.{Map, HashMap}
import java.util.{List, ArrayList}
import java.io.{File, RandomAccessFile}

import com.fasterxml.jackson.databind.{SerializationFeature, DeserializationFeature, ObjectMapper}

import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

trait KtVtCollection {
  def put(key: String, field: String, value: String)
  def putAll(key: String, more: Map[String, String])
  def get(key: String, field: String, defaultValue: String = null): String
  def getAll(key: String, defaultValue: Map[String, String] = null): Map[String, String]
  def getKeys(): List[String]
  def remove(key: String, field: String)
  def removeAll(key: String)
}

trait KtVtDatabase {
  def getLocalCollection(): KtVtCollection
  def getCollectionNames(): List[String]
  def getCollection(name: String): KtVtCollection
  def dropCollection(name: String)
  def sync()
}

object KtvtJsonMapper {
  private val jsonMapper = new ObjectMapper()
  jsonMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
  jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def readValue[T](src: File): HashMap[String, HashMap[String, String]] = {
    jsonMapper.readValue(src, classOf[HashMap[String, HashMap[String, String]]])
  }

  def writeValue(dst: File, value: HashMap[String, HashMap[String, String]]) {
    jsonMapper.writeValue(dst, value)
  }
}

class KtVtFileCollection(file: File) extends KtVtCollection {
  private val logger = LoggerFactory.getLogger(getClass().getName())

  @volatile private var updated = false
  private val lock = new Object()
  private val kves: HashMap[String, HashMap[String, String]] = {
    if(!file.exists()) {
      updated = true;
      new HashMap[String, HashMap[String, String]]()
    }
    else
      try {
        KtvtJsonMapper.readValue(file);
      } catch {
        case e: Throwable => {
          logger.error("open file failed", e)
          new HashMap[String, HashMap[String, String]]()
        }
      }
  }

  def put(key: String, field: String, value: String) {
    lock.synchronized {
      updated = true
      val fields = kves.get(key)
      if(fields != null)
        fields.put(field, value)
      else
        kves.put(key, new HashMap[String, String]() {
          put(field, value)
        })
    }
  }

  def putAll(key: String, more: Map[String, String]) {
    lock.synchronized {
      updated = true
      val fields = kves.get(key)
      if(fields != null)
        fields.putAll(more)
      else
        kves.put(key, new HashMap[String, String](more))
    }
  }

  def get(key: String, field: String, defaultValue: String = null): String = {
    lock.synchronized {
      val fields = kves.get(key)
      if(fields == null)
        defaultValue
      else {
        val value = fields.get(field)
        if(value != null)
          value
        else
          defaultValue
      }
    }
  }

  def getAll(key: String, defaultValue: Map[String, String] = null): Map[String, String] = {
    lock.synchronized {
      val fields = kves.get(key)
      if(fields == null)
        defaultValue
      else
        new HashMap[String, String](fields)
    }
  }

  def getKeys(): List[String] = {
    lock.synchronized {
      new ArrayList[String]() {
        for(key <- kves.keySet().asScala)
          add(key)
      }
    }
  }

  def getFields(key: String): List[String] = {
    lock.synchronized {
      new ArrayList[String]() {
        val fields = kves.get(key)
        if(fields != null) {
          for(key <- fields.keySet().asScala)
            add(key)
        }
      }
    }
  }

  def remove(key: String, field: String) {
    lock.synchronized {
      val fields = kves.get(key)
      if(fields != null) {
        updated = true
        fields.remove(field)
      }
    }
  }

  def removeAll(key: String) {
    lock.synchronized {
      updated = true
      kves.remove(key)
    }
  }

  def syncToFile() {
    lock.synchronized {
      if(updated) {
        try {
          KtvtJsonMapper.writeValue(file, kves)
        } catch {
          case e: Throwable => {
            logger.error("sync database failed", e)
          }
        }
      }
    }
  }
}

class KtVtFileDatabase(path: String) extends KtVtDatabase {
  private val COLLECTION_EXT_NAME = ".ktvt2"
  private val COLLECTION_KEY_NAME = "collections"

  private val root = {
    val root = new File(path)
    if(!root.exists())
      root.mkdirs()
    else if(!root.isDirectory())
      throw new IllegalStateException("Target path not a directory: " + root.getAbsolutePath())
    root
  }

  private val lockFile = {
    var fileLock: Any = null
    val lockFile = new RandomAccessFile(new File(root, ".lockfile2"), "rw")
    try {
      fileLock = lockFile.getChannel().tryLock()
    } catch {
      case _: Throwable => {}
    }
    if(fileLock == null)
      throw new IllegalStateException("Target path multiple opened: " + root.getAbsolutePath())
    lockFile
  }

  private def getFile(name: String):File = {
    new File(root, name + COLLECTION_EXT_NAME)
  }

  private val collLock = new Object()
  private val syncLock = new Object()
  private val collections = new HashMap[String, KtVtFileCollection]()
  private val localCollection = new KtVtFileCollection(getFile("local"))
  private val systemCollection = new KtVtFileCollection(getFile("system"))

  def getLocalCollection(): KtVtCollection = {
    localCollection
  }

  def getCollection(name: String): KtVtCollection = {
    collLock.synchronized {
      if(collections.containsKey(name))
        collections.get(name)
      else {
        val collection = new KtVtFileCollection(getFile(SomeUtil.sha1(name)))
        collections.put(name, collection)
        systemCollection.put(COLLECTION_KEY_NAME, name, SomeUtil.getTimestamp().toString)
        collection
      }
    }
  }

  def getCollectionNames(): List[String] = {
    systemCollection.getFields(COLLECTION_KEY_NAME)
  }

  def dropCollection(name: String) {
    collLock.synchronized {
      collections.remove(name)
      systemCollection.remove(COLLECTION_KEY_NAME, name)
      getFile(SomeUtil.sha1(name)).delete()
    }
  }

  private def syncToFile() {
    localCollection.syncToFile()
    systemCollection.syncToFile()
    collLock.synchronized {
      for((name, collection) <- collections.asScala)
        collection.syncToFile()
    }
  }

  def sync() {
    syncLock.synchronized {
      syncToFile()
    }
  }

  val syncThread = new Thread(new Runnable() {
    override def run() {
      while(true) {
        Thread.sleep(500)
        syncLock.synchronized {
          syncToFile()
        }
      }
    }
  })
  syncThread.setDaemon(true)
  syncThread.start()
}

object KtVtStore {
  def openOrCreate(path: String): KtVtDatabase = {
    new KtVtFileDatabase(path)
  }
}
