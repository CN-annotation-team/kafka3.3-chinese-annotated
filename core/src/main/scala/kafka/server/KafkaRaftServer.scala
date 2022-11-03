/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.io.File
import java.util.concurrent.CompletableFuture
import kafka.common.InconsistentNodeIdException
import kafka.log.{LogConfig, UnifiedLog}
import kafka.metrics.KafkaMetricsReporter
import kafka.raft.KafkaRaftManager
import kafka.server.KafkaRaftServer.{BrokerRole, ControllerRole}
import kafka.server.metadata.BrokerServerMetrics
import kafka.utils.{CoreUtils, Logging, Mx4jLoader, VerifiableProperties}
import org.apache.kafka.common.config.{ConfigDef, ConfigResource}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.utils.{AppInfoParser, Time}
import org.apache.kafka.common.{KafkaException, Uuid}
import org.apache.kafka.controller.QuorumControllerMetrics
import org.apache.kafka.metadata.bootstrap.{BootstrapDirectory, BootstrapMetadata}
import org.apache.kafka.metadata.{KafkaConfigSchema, MetadataRecordSerde}
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.server.fault.{LoggingFaultHandler, ProcessExitingFaultHandler}
import org.apache.kafka.server.metrics.KafkaYammerMetrics

import java.util.Optional
import scala.collection.Seq
import scala.jdk.CollectionConverters._

/**
 * This class implements the KRaft (Kafka Raft) mode server which relies
 * on a KRaft quorum for maintaining cluster metadata. It is responsible for
 * constructing the controller and/or broker based on the `process.roles`
 * configuration and for managing their basic lifecycle (startup and shutdown).
 */
/**
 * 该类中主要管理了三个实例对象
 * raftManager
 * broker
 * controller
 */
class KafkaRaftServer(
  config: KafkaConfig,
  time: Time,
  threadNamePrefix: Option[String]
) extends Server with Logging {

  this.logIdent = s"[KafkaRaftServer nodeId=${config.nodeId}] "
  KafkaMetricsReporter.startReporters(VerifiableProperties(config.originals))
  KafkaYammerMetrics.INSTANCE.configure(config.originals)

  // 初始化 kafka 的日志目录信息
  // 这里分别是
  // meta.properties 文件的配置信息，
  // bootstrap.checkpoint 文件的信息
  // 离线目录，即 log.dirs 这些目录下的 meta.properties 解析失败，就会认为是离线目录
  private val (metaProps, bootstrapMetadata, offlineDirs) = KafkaRaftServer.initializeLogDirs(config)

  private val metrics = Server.initializeMetrics(
    config,
    time,
    metaProps.clusterId
  )

  // 定义一个解析集群投票节点配置的任务，config.quorumVoters 就是 id@ip:port 集合
  private val controllerQuorumVotersFuture = CompletableFuture.completedFuture(
    RaftConfig.parseVoterConnections(config.quorumVoters))

  // 初始化一个 KafkaRaftManager 实例
  private val raftManager = new KafkaRaftManager[ApiMessageAndVersion](
    // meta.properties 文件的属性信息
    metaProps,
    // 当前节点的配置信息
    config,
    // 元数据的反序列化器
    new MetadataRecordSerde,
    // 元数据 topic Partition，这个 topic 名是 __cluster_metadata，分区为 0
    KafkaRaftServer.MetadataPartition,
    // 元数据的 topic id
    KafkaRaftServer.MetadataTopicId,
    // 系统时间
    time,
    // 指标统计工具
    metrics,
    // 线程名前缀，没有指定
    threadNamePrefix,
    // 集群参与投票的节点的配置解析任务
    controllerQuorumVotersFuture
  )

  /**
   * 下面会根据配置文件中配置的 process.roles 来实例化对应的角色
   * kraft 下 kafka角色分两种 controller,broker。这两种角色都有自己的开放端口
   * 1. controller：controller 是使用 kraft 来做节点之间的选举，决策
   * 2. broker：broker 是用于处理数据以及节点之间的交流
   *
   * 详细说一下怎么区分 controller 和 broker 具体处理哪些对外开放的接口
   * 1.
   * controller 角色对应的接口类是 {@link ControllerApis}
   * broker 角色对应的接口类是 {@link KafkaApis}
   * 2.
   * controller 和 broker 都会实例化一个 SocketServer 实例，在实例化 SocketServer 会根据两种角色
   * 分别传入 {@link org.apache.kafka.common.message.ApiMessageType.ListenerType.CONTROLLER}
   * 和 {@link org.apache.kafka.common.message.ApiMessageType.ListenerType.BROKER}
   * 3.
   * SocketServer 里面有两中网络通信类型，分别是 data,controller。如果实例化时传入的 ListenerType
   * 是 CONTROLLER，则只会启用 controller 类型的网络通信，如果是 BROKER 在 kraft 下会启用 data 类型的网络通信
   * 4.
   * 定位到 {@link ControllerApis.handle()} 和 {@link KafkaApis.handle()} 方法，里面就是两种角色
   * 各自要处理的 api，根据其中的 {@link ApiKeys} 然后定位到 {@link org.apache.kafka.common.message.ApiMessageType}
   * 注意这个类是通过 json 文件生成的，只有 class,没有对应的 java 文件。
   * ApiMessageType 中定义的所有 api 的最后一个属性会指定这个 api 会被哪些类型的网络通信处理
   * 5.
   * 对于这些 api，只需要关注 CONTROLLER 和 BROKER 两种，现在就可以分三种情况来看
   *    5.1）只有 CONTROLLER，这种 api 只会由 controller 角色来处理，也就是 controller 角色之间相互交流，基本就是集群选举，controller 心跳等
   *    5.2）只有 BROKER，这种基本就是生产消费数据等相关的
   *    5.3）即有 BROKER,又有 CONTROLLER，这种情况下就是如果处理的角色 BROKER，可以认为这个是 controller 客户端，如果处理的角色是 CONTROLLER，认为是 controller 服务端
   */


  // broker 角色实例化，会判断 processRoles 中是否包含了 broker 角色
  private val broker: Option[BrokerServer] = if (config.processRoles.contains(BrokerRole)) {
    val brokerMetrics = BrokerServerMetrics(metrics)
    val fatalFaultHandler = new ProcessExitingFaultHandler()
    val metadataLoadingFaultHandler = new LoggingFaultHandler("metadata loading",
        () => brokerMetrics.metadataLoadErrorCount.getAndIncrement())
    val metadataApplyingFaultHandler = new LoggingFaultHandler("metadata application",
      () => brokerMetrics.metadataApplyErrorCount.getAndIncrement())
    Some(new BrokerServer(
      config,
      metaProps,
      raftManager,
      time,
      metrics,
      brokerMetrics,
      threadNamePrefix,
      offlineDirs,
      controllerQuorumVotersFuture,
      fatalFaultHandler,
      metadataLoadingFaultHandler,
      metadataApplyingFaultHandler
    ))
  } else {
    None
  }

  // controller 角色实例化，会判断 processRoles 中是否包含了 controller 角色
  private val controller: Option[ControllerServer] = if (config.processRoles.contains(ControllerRole)) {
    val controllerMetrics = new QuorumControllerMetrics(KafkaYammerMetrics.defaultRegistry(), time)
    val metadataFaultHandler = new LoggingFaultHandler("controller metadata",
      () => controllerMetrics.incrementMetadataErrorCount())
    val fatalFaultHandler = new ProcessExitingFaultHandler()
    Some(new ControllerServer(
      metaProps,
      config,
      raftManager,
      time,
      metrics,
      controllerMetrics,
      threadNamePrefix,
      controllerQuorumVotersFuture,
      KafkaRaftServer.configSchema,
      raftManager.apiVersions,
      bootstrapMetadata,
      metadataFaultHandler,
      fatalFaultHandler
    ))
  } else {
    None
  }

  override def startup(): Unit = {
    Mx4jLoader.maybeLoad()
    // Note that we startup `RaftManager` first so that the controller and broker
    // can register listeners during initialization.
    raftManager.startup()
    controller.foreach(_.startup())
    broker.foreach(_.startup())
    AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
    info(KafkaBroker.STARTED_MESSAGE)
  }

  override def shutdown(): Unit = {
    broker.foreach(_.shutdown())
    // The order of shutdown for `RaftManager` and `ControllerServer` is backwards
    // compared to `startup()`. This is because the `SocketServer` implementation that
    // we rely on to receive requests is owned by `ControllerServer`, so we need it
    // to stick around until graceful shutdown of `RaftManager` can be completed.
    raftManager.shutdown()
    controller.foreach(_.shutdown())
    CoreUtils.swallow(AppInfoParser.unregisterAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics), this)

  }

  override def awaitShutdown(): Unit = {
    broker.foreach(_.awaitShutdown())
    controller.foreach(_.awaitShutdown())
  }

}

object KafkaRaftServer {
  // 元数据的 topic 名 __cluster_metadata
  val MetadataTopic = Topic.METADATA_TOPIC_NAME
  // 元数据的 topic 分区为 0
  val MetadataPartition = Topic.METADATA_TOPIC_PARTITION
  val MetadataTopicId = Uuid.METADATA_TOPIC_ID

  sealed trait ProcessRole
  case object BrokerRole extends ProcessRole
  case object ControllerRole extends ProcessRole

  /**
   * Initialize the configured log directories, including both [[KafkaConfig.MetadataLogDirProp]]
   * and [[KafkaConfig.LogDirProp]]. This method performs basic validation to ensure that all
   * directories are accessible and have been initialized with consistent `meta.properties`.
   *
   * @param config The process configuration
   * @return A tuple containing the loaded meta properties (which are guaranteed to
   *         be consistent across all log dirs) and the offline directories
   */
  def initializeLogDirs(config: KafkaConfig): (MetaProperties, BootstrapMetadata, Seq[String]) = {
    // 这里会获取 log.dirs 与 metadata.log.dir 目录的并集目录
    val logDirs = (config.logDirs.toSet + config.metadataLogDir).toSeq
    // 获取 meta.properties 文件属性信息，以及离线的目录（即目录下的meta.properties读取出错）
    val (rawMetaProperties, offlineDirs) = BrokerMetadataCheckpoint.
      getBrokerMetadataAndOfflineDirs(logDirs, ignoreMissing = false)

    // metadata.log.dir 指定的目录也是离线目录，会报错
    if (offlineDirs.contains(config.metadataLogDir)) {
      throw new KafkaException("Cannot start server since `meta.properties` could not be " +
        s"loaded from ${config.metadataLogDir}")
    }

    val metadataPartitionDirName = UnifiedLog.logDirName(MetadataPartition)
    val onlineNonMetadataDirs = logDirs.diff(offlineDirs :+ config.metadataLogDir)
    onlineNonMetadataDirs.foreach { logDir =>
      val metadataDir = new File(logDir, metadataPartitionDirName)
      if (metadataDir.exists) {
        throw new KafkaException(s"Found unexpected metadata location in data directory `$metadataDir` " +
          s"(the configured metadata directory is ${config.metadataLogDir}).")
      }
    }

    // 将 rawMetaProperties 解析成 MetaProperties 类型
    val metaProperties = MetaProperties.parse(rawMetaProperties)
    // 如果配置文件指定的 node.id 和 meta.properties 中的 node.id 不同，报错
    if (config.nodeId != metaProperties.nodeId) {
      throw new InconsistentNodeIdException(
        s"Configured node.id `${config.nodeId}` doesn't match stored node.id `${metaProperties.nodeId}' in " +
          "meta.properties. If you moved your data, make sure your configured controller.id matches. " +
          "If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    }

    // 这里是读取 bootstrap.checkpoint 文件，这个文件在使用 kafka-storage.sh 时会和 meta.properties 一起生成
    val bootstrapDirectory = new BootstrapDirectory(config.metadataLogDir,
      Optional.ofNullable(config.interBrokerProtocolVersionString))
    val bootstrapMetadata = bootstrapDirectory.read()

    (metaProperties, bootstrapMetadata, offlineDirs.toSeq)
  }

  val configSchema = new KafkaConfigSchema(Map(
    ConfigResource.Type.BROKER -> new ConfigDef(KafkaConfig.configDef),
    ConfigResource.Type.TOPIC -> LogConfig.configDefCopy,
  ).asJava, LogConfig.AllTopicConfigSynonyms)
}
