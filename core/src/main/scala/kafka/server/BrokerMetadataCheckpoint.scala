/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server

import java.io._
import java.nio.file.{Files, NoSuchFileException}
import java.util.Properties

import kafka.common.InconsistentBrokerMetadataException
import kafka.server.RawMetaProperties._
import kafka.utils._
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * 定义了 meta.properties 文件中的属性，
 * 使用 Kafka-storage.sh 会在每个日志目录下生成一个 meta.properties 文件和一个 bootstrap.checkpoint 文件
 * broker.id 是使用 zookeeper 时使用到的
 * node.id 是使用 kraft 是使用到的
 */
object RawMetaProperties {
  val ClusterIdKey = "cluster.id"
  val BrokerIdKey = "broker.id"
  val NodeIdKey = "node.id"
  val VersionKey = "version"
}

class RawMetaProperties(val props: Properties = new Properties()) {

  def clusterId: Option[String] = {
    Option(props.getProperty(ClusterIdKey))
  }

  def clusterId_=(id: String): Unit = {
    props.setProperty(ClusterIdKey, id)
  }

  def brokerId: Option[Int] = {
    intValue(BrokerIdKey)
  }

  def brokerId_=(id: Int): Unit = {
    props.setProperty(BrokerIdKey, id.toString)
  }

  def nodeId: Option[Int] = {
    intValue(NodeIdKey)
  }

  def nodeId_=(id: Int): Unit = {
    props.setProperty(NodeIdKey, id.toString)
  }

  def version: Int = {
    intValue(VersionKey).getOrElse(0)
  }

  def version_=(ver: Int): Unit = {
    props.setProperty(VersionKey, ver.toString)
  }

  def requireVersion(expectedVersion: Int): Unit = {
    if (version != expectedVersion) {
      throw new RuntimeException(s"Expected version $expectedVersion, but got "+
        s"version $version")
    }
  }

  private def intValue(key: String): Option[Int] = {
    try {
      Option(props.getProperty(key)).map(Integer.parseInt)
    } catch {
      case e: Throwable => throw new RuntimeException(s"Failed to parse $key property " +
        s"as an int: ${e.getMessage}")
    }
  }

  override def equals(that: Any): Boolean = that match {
    case other: RawMetaProperties => props.equals(other.props)
    case _ => false
  }

  override def hashCode(): Int = props.hashCode

  override def toString: String = {
    "{" + props.keySet().asScala.toList.asInstanceOf[List[String]].sorted.map {
      key => key + "=" + props.get(key)
    }.mkString(", ") + "}"
  }
}

object MetaProperties {
  def parse(properties: RawMetaProperties): MetaProperties = {
    properties.requireVersion(expectedVersion = 1)
    val clusterId = require(ClusterIdKey, properties.clusterId)
    val nodeId = require(NodeIdKey, properties.nodeId)
    new MetaProperties(clusterId, nodeId)
  }

  def require[T](key: String, value: Option[T]): T = {
    value.getOrElse(throw new RuntimeException(s"Failed to find required property $key."))
  }
}

// 使用 zookeeper 的时候 meta.properties 文件的属性信息
case class ZkMetaProperties(
  clusterId: String,
  brokerId: Int
) {
  def toProperties: Properties = {
    val properties = new RawMetaProperties()
    properties.version = 0
    properties.clusterId = clusterId
    properties.brokerId = brokerId
    properties.props
  }

  override def toString: String = {
    s"ZkMetaProperties(brokerId=$brokerId, clusterId=$clusterId)"
  }
}

// kraft 下的 meta.properties
case class MetaProperties(
  clusterId: String,
  nodeId: Int,
) {
  // 可以看到有 version,cluster.id,node.id 三个属性
  def toProperties: Properties = {
    val properties = new RawMetaProperties()
    properties.version = 1
    properties.clusterId = clusterId
    properties.nodeId = nodeId
    properties.props
  }

  override def toString: String  = {
    s"MetaProperties(clusterId=$clusterId, nodeId=$nodeId)"
  }
}

object BrokerMetadataCheckpoint extends Logging {
  def getBrokerMetadataAndOfflineDirs(
    logDirs: collection.Seq[String],
    ignoreMissing: Boolean
  ): (RawMetaProperties, collection.Seq[String]) = {
    require(logDirs.nonEmpty, "Must have at least one log dir to read meta.properties")

    val brokerMetadataMap = mutable.HashMap[String, Properties]()
    val offlineDirs = mutable.ArrayBuffer.empty[String]

    // 遍历给定的 logDirs
    for (logDir <- logDirs) {
      // 获取每个目录下的 meta.properties 文件
      // meta.properties 文件中存储了三个属性 cluster.id, version, node.id（kraft）/broker.id（zookeeper）
      // 通过这三个属性可以唯一确定一个集群中的节点
      val brokerCheckpointFile = new File(logDir, "meta.properties")
      val brokerCheckpoint = new BrokerMetadataCheckpoint(brokerCheckpointFile)

      try {
        // 读取 meta.properties 文件
        brokerCheckpoint.read() match {
          case Some(properties) =>
            // 用 map 存储 logDir 和 meta.properties文件中信息 的映射
            brokerMetadataMap += logDir -> properties
          case None =>
            // logDir 下没有这个 meta.properties 文件，就会报错，
            // 需要使用 kafka-storage.sh 来生成这个文件
            if (!ignoreMissing) {
              throw new KafkaException(s"No `meta.properties` found in $logDir " +
                "(have you run `kafka-storage.sh` to format the directory?)")
            }
        }
      } catch {
        // 解析 meta.properties 成 Properties 实例出错，则将该目录认为是离线目录
        case e: IOException =>
          offlineDirs += logDir
          error(s"Failed to read $brokerCheckpointFile", e)
      }
    }

    if (brokerMetadataMap.isEmpty) {
      // 没有成功读取到任何一个 meta.properties的 的情况
      (new RawMetaProperties(), offlineDirs)
    } else {
      // 对 meta.properties 中的信息进行唯一性校验，判断当前节点的所有meta.properties是否相同，
      // meta.properties 会唯一确定一个集群中的节点，如果meta.properties 不同，表示可能其中一些日志
      // 目录是从其他节点复制过来了，没有更改 meta.properties 文件
      val numDistinctMetaProperties = brokerMetadataMap.values.toSet.size
      if (numDistinctMetaProperties > 1) {
        val builder = new StringBuilder

        for ((logDir, brokerMetadata) <- brokerMetadataMap)
          builder ++= s"- $logDir -> $brokerMetadata\n"

        throw new InconsistentBrokerMetadataException(
          s"BrokerMetadata is not consistent across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
            s"or partial data was manually copied from another broker. Found:\n${builder.toString()}"
        )
      }

      val rawProps = new RawMetaProperties(brokerMetadataMap.head._2)
      (rawProps, offlineDirs)
    }
  }
}

/**
 * This class saves the metadata properties to a file
 */
/* 这个类用来读写元数据属性信息 */
class BrokerMetadataCheckpoint(val file: File) extends Logging {
  private val lock = new Object()

  // 写数据到文件
  def write(properties: Properties): Unit = {
    lock synchronized {
      try {
        // 生成一个临时文件
        val temp = new File(file.getAbsolutePath + ".tmp")
        val fileOutputStream = new FileOutputStream(temp)
        try {
          // 将数据存储到临时文件中
          properties.store(fileOutputStream, "")
          // 刷盘
          fileOutputStream.flush()
          fileOutputStream.getFD.sync()
        } finally {
          Utils.closeQuietly(fileOutputStream, temp.getName)
        }
        // 将临时文件改成 metadata 文件
        Utils.atomicMoveWithFallback(temp.toPath, file.toPath)
      } catch {
        case ie: IOException =>
          error("Failed to write meta.properties due to", ie)
          throw ie
      }
    }
  }

  // 读取文件信息
  def read(): Option[Properties] = {
    // 如果该文件存在临时文件，删除该临时文件
    Files.deleteIfExists(new File(file.getPath + ".tmp").toPath) // try to delete any existing temp files for cleanliness

    val absolutePath = file.getAbsolutePath
    lock synchronized {
      try {
        // 加载文件成一个 Properties
        Some(Utils.loadProps(absolutePath))
      } catch {
        case _: NoSuchFileException =>
          warn(s"No meta.properties file under dir $absolutePath")
          None
        case e: Exception =>
          error(s"Failed to read meta.properties file under dir $absolutePath", e)
          throw e
      }
    }
  }
}
