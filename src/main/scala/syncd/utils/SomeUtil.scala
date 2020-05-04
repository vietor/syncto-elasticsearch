package syncd.utils

import java.io._
import java.util.ArrayList
import java.security.MessageDigest
import java.nio.charset.StandardCharsets

import scala.io._

object SomeUtil {

  def sha1(s: String): String = {
    MessageDigest.getInstance("SHA1")
      .digest(s.getBytes(StandardCharsets.UTF_8))
      .map("%02x".format(_)).mkString
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
