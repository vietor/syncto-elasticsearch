package syncd.utils

import java.io.File
import com.fasterxml.jackson.databind.{JsonNode, SerializationFeature, DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonUtil {
  private val jsonMapper = new ObjectMapper()
  jsonMapper.registerModule(DefaultScalaModule)
  jsonMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
  jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def createObjectNode(): ObjectNode = {
    jsonMapper.createObjectNode()
  }

  def readTree(src: File): JsonNode = {
    jsonMapper.readTree(src)
  }

  def writeValue(dst: File, value: JsonNode) = {
    jsonMapper.writeValue(dst, value)
  }

  def writeValuePretty(dst: File, value: JsonNode) = {
    jsonMapper.writerWithDefaultPrettyPrinter().writeValue(dst, value)
  }

  def readTree(src: String): JsonNode = {
    jsonMapper.readTree(src)
  }

  def writeValueAsString(value: JsonNode): String = {
    jsonMapper.writeValueAsString(value)
  }

  def writeValueAsPrettyString(value: JsonNode): String = {
    jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(value)
  }

  def readValue[T](src: File, valueType: Class[T]): T = {
    jsonMapper.readValue(src, valueType)
  }

  def writeValue[T](dst: File, value: T) = {
    jsonMapper.writeValue(dst, value)
  }

  def writeValuePretty[T](dst: File, value: T) = {
    jsonMapper.writerWithDefaultPrettyPrinter().writeValue(dst, value)
  }

  def readValue[T](src: String, valueType: Class[T]): T = {
    jsonMapper.readValue(src, valueType)
  }

  def writeValueAsString[T](value: T): String = {
    jsonMapper.writeValueAsString(value)
  }

  def writeValueAsPrettyString[T](value: T): String = {
    jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(value)
  }

  def convertValue[T](src: JsonNode, valueType: Class[T]): T = {
    jsonMapper.convertValue(src, valueType)
  }

  def convertValue[T](src: java.util.Map[_, _], valueType: Class[T]): T = {
    jsonMapper.convertValue(src, valueType)
  }

}
