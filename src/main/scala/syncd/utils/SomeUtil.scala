package syncd.utils

import java.io._
import java.util.ArrayList
import java.security.MessageDigest

import scala.io._

object SomeUtil {

  def md5(s: String): String = {
    val hash = MessageDigest.getInstance("MD5").digest(s.getBytes)
    hash.map("%02x".format(_)).mkString
  }

  def sha1(s: String): String = {
    val hash = MessageDigest.getInstance("SHA1").digest(s.getBytes)
    hash.map("%02x".format(_)).mkString
  }

  def getTimestamp(): Long = {
    System.currentTimeMillis / 1000
  }

  def tryFindFile(paths: Array[String], name: String): String = {
    val exists = new ArrayList[String]()
    for(path <- paths) {
      val file = new File(path + name)
      if(file.exists)
        exists.add(file.getAbsolutePath())
    }
    if(exists.size() < 1)
      null
    else
      exists.get(0)
  }

  def readFileAsString(path: String): String = {
    Source.fromFile(path).mkString
  }

  def readFileAsString(paths: Array[String], name: String): String = {
    val file = tryFindFile(paths, name)
    if(file == null)
      null
    else
      Source.fromFile(file).mkString
  }
}
