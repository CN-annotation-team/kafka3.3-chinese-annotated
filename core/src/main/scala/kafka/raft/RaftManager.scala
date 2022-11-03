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
package kafka.raft

import java.io.File
import java.nio.file.Files
import java.util
import java.util.OptionalInt
import java.util.concurrent.CompletableFuture
import kafka.log.UnifiedLog
import kafka.raft.KafkaRaftManager.RaftIoThread
import kafka.server.{KafkaConfig, MetaProperties}
import kafka.utils.timer.SystemTimer
import kafka.utils.{KafkaScheduler, Logging, ShutdownableThread}
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, ListenerName, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.raft.RaftConfig.{AddressSpec, InetAddressSpec, NON_ROUTABLE_ADDRESS, UnknownAddressSpec}
import org.apache.kafka.raft.{FileBasedStateStore, KafkaRaftClient, LeaderAndEpoch, RaftClient, RaftConfig, RaftRequest, ReplicatedLog}
import org.apache.kafka.server.common.serialization.RecordSerde
import scala.jdk.CollectionConverters._

object KafkaRaftManager {
  class RaftIoThread(
    client: KafkaRaftClient[_],
    threadNamePrefix: String
  ) extends ShutdownableThread(
    name = threadNamePrefix + "-io-thread",
    isInterruptible = false
  ) {
    // 该 raftIoThread 启动之后会一直调用该方法
    override def doWork(): Unit = {
      client.poll()
    }

    override def initiateShutdown(): Boolean = {
      if (super.initiateShutdown()) {
        client.shutdown(5000).whenComplete { (_, exception) =>
          if (exception != null) {
            error("Graceful shutdown of RaftClient failed", exception)
          } else {
            info("Completed graceful shutdown of RaftClient")
          }
        }
        true
      } else {
        false
      }
    }

    override def isRunning: Boolean = {
      client.isRunning && !isThreadFailed
    }
  }

  private def createLogDirectory(logDir: File, logDirName: String): File = {
    val logDirPath = logDir.getAbsolutePath
    val dir = new File(logDirPath, logDirName)
    Files.createDirectories(dir.toPath)
    dir
  }
}

trait RaftManager[T] {
  def handleRequest(
    header: RequestHeader,
    request: ApiMessage,
    createdTimeMs: Long
  ): CompletableFuture[ApiMessage]

  def register(
    listener: RaftClient.Listener[T]
  ): Unit

  def leaderAndEpoch: LeaderAndEpoch

  def client: RaftClient[T]

  def replicatedLog: ReplicatedLog
}

/**
 * 在阅读 kafka raft 部分的源码之前，需要先了解以下知识点
 * raft 是分布式共识算法，可以用来做节点之间的选举，
 * 而 kafka 里面的选举分为两种
 * 1. Controller 角色选举 leader 节点，在 kraft 下我们会指定集群中的一些节点为 controller 角色
 *    在配置文件中会设置 controller.quorum.voters 来指定哪些 controller 可以参与投票选举 leader
 *    这里的 controller 角色选举 leader 节点就是使用 raft 算法来实现
 * 2. topic 的分区副本选举，这个选举 leader 副本是让 leader controller 节点来进行选择
 */

class KafkaRaftManager[T](
  metaProperties: MetaProperties,
  config: KafkaConfig,
  recordSerde: RecordSerde[T],
  topicPartition: TopicPartition,
  topicId: Uuid,
  time: Time,
  metrics: Metrics,
  threadNamePrefixOpt: Option[String],
  val controllerQuorumVotersFuture: CompletableFuture[util.Map[Integer, AddressSpec]]
) extends RaftManager[T] with Logging {

  val apiVersions = new ApiVersions()
  // 获取 raft 相关配置
  private val raftConfig = new RaftConfig(config)
  private val threadNamePrefix = threadNamePrefixOpt.getOrElse("kafka-raft")
  private val logContext = new LogContext(s"[RaftManager nodeId=${config.nodeId}] ")
  this.logIdent = logContext.logPrefix()

  // 创建一个线程池，该线程池中线程数为 1
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix + "-scheduler")
  // 启动线程池
  scheduler.startup()

  // 创建元数据 topic partition 目录，topic 名是 __cluster_metadata，分区为 0
  // 会在 logDirs 下创建一个 __cluster_metadata-0 目录
  private val dataDir = createDataDir()
  // 创建集群元数据副本日志，这个副本就是上面 topic 为 __cluster_metadata，分区为 0 的这个分区的副本
  // 每个节点都有一份相同的这个副本日志用来记录集群所有节点的状态信息，既然所有节点都有这个副本，那么我们在
  // 选举的时候就可以通过这个副本日志偏移量来判断哪个节点的集群信息更全面
  override val replicatedLog: ReplicatedLog = buildMetadataLog()
  private val netChannel = buildNetworkChannel()
  private val expirationTimer = new SystemTimer("raft-expiration-executor")
  private val expirationService = new TimingWheelExpirationService(expirationTimer)
  // 创建一个 kraft 客户端
  override val client: KafkaRaftClient[T] = buildRaftClient()
  // raft 网络 IO 线程持有 raft 客户端，这个线程是 TCP 客户端，服务于 raft 客户端
  private val raftIoThread = new RaftIoThread(client, threadNamePrefix)

  def startup(): Unit = {
    // Update the voter endpoints (if valid) with what's in RaftConfig
    // 获取 brokerId => address map
    val voterAddresses: util.Map[Integer, AddressSpec] = controllerQuorumVotersFuture.get()
    for (voterAddressEntry <- voterAddresses.entrySet.asScala) {
      voterAddressEntry.getValue match {
        // 更新当前创建的网络通道中的 broker 对应的地址
        case spec: InetAddressSpec =>
          netChannel.updateEndpoint(voterAddressEntry.getKey, spec)
        case _: UnknownAddressSpec =>
          info(s"Skipping channel update for destination ID: ${voterAddressEntry.getKey} " +
            s"because of non-routable endpoint: ${NON_ROUTABLE_ADDRESS.toString}")
        case invalid: AddressSpec =>
          warn(s"Unexpected address spec (type: ${invalid.getClass}) for channel update for " +
            s"destination ID: ${voterAddressEntry.getKey}")
      }
    }
    // 启动网络通道
    netChannel.start()
    // 启动 raft 网络通信线程
    raftIoThread.start()
  }

  def shutdown(): Unit = {
    expirationService.shutdown()
    expirationTimer.shutdown()
    raftIoThread.shutdown()
    client.close()
    scheduler.shutdown()
    netChannel.close()
    replicatedLog.close()
  }

  override def register(
    listener: RaftClient.Listener[T]
  ): Unit = {
    client.register(listener)
  }

  // 处理 raft 请求
  override def handleRequest(
    header: RequestHeader,
    request: ApiMessage,
    createdTimeMs: Long
  ): CompletableFuture[ApiMessage] = {
    val inboundRequest = new RaftRequest.Inbound(
      header.correlationId,
      request,
      createdTimeMs
    )

    // 调用 client 处理请求，这里其实只是将请求入队
    client.handle(inboundRequest)

    inboundRequest.completion.thenApply { response =>
      response.data
    }
  }

  // 创建一个 kraft 客户端
  private def buildRaftClient(): KafkaRaftClient[T] = {
    // 实例化 quorum 状态信息存储器，指定存储的文件是 logDirs 下的 __cluster_metadata-0 目录下的 quorum-state 文件
    val quorumStateStore = new FileBasedStateStore(new File(dataDir, "quorum-state"))
    val nodeId = OptionalInt.of(config.nodeId)

    // 实例化 KafkaRaftClient
    val client = new KafkaRaftClient(
      recordSerde,
      netChannel,
      replicatedLog,
      quorumStateStore,
      time,
      metrics,
      expirationService,
      logContext,
      metaProperties.clusterId,
      nodeId,
      raftConfig
    )
    // 调用其初始化方法，进行当前节点的 quorum 状态初始化
    client.initialize()
    client
  }

  private def buildNetworkChannel(): KafkaNetworkChannel = {
    val netClient = buildNetworkClient()
    new KafkaNetworkChannel(time, netClient, config.quorumRequestTimeoutMs, threadNamePrefix)
  }

  // 创建 __cluster_metadata-0 目录
  private def createDataDir(): File = {
    val logDirName = UnifiedLog.logDirName(topicPartition)
    KafkaRaftManager.createLogDirectory(new File(config.metadataLogDir), logDirName)
  }

  // 创建 kafka 集群元数据日志实例
  private def buildMetadataLog(): KafkaMetadataLog = {
    KafkaMetadataLog(
      topicPartition,
      topicId,
      dataDir,
      time,
      scheduler,
      config = MetadataLogConfig(config, KafkaRaftClient.MAX_BATCH_SIZE_BYTES, KafkaRaftClient.MAX_FETCH_SIZE_BYTES)
    )
  }

  // 构建网络客户端，这个就是用来向 controller 节点发送请求的 TCP 客户端
  private def buildNetworkClient(): NetworkClient = {
    val controllerListenerName = new ListenerName(config.controllerListenerNames.head)
    val controllerSecurityProtocol = config.effectiveListenerSecurityProtocolMap.getOrElse(controllerListenerName, SecurityProtocol.forName(controllerListenerName.value()))
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      controllerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      controllerListenerName,
      config.saslMechanismControllerProtocol,
      time,
      config.saslInterBrokerHandshakeRequestEnable,
      logContext
    )

    val metricGroupPrefix = "raft-channel"
    val collectPerConnectionMetrics = false

    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      config.connectionsMaxIdleMs,
      metrics,
      time,
      metricGroupPrefix,
      Map.empty[String, String].asJava,
      collectPerConnectionMetrics,
      channelBuilder,
      logContext
    )

    val clientId = s"raft-client-${config.nodeId}"
    val maxInflightRequestsPerConnection = 1
    val reconnectBackoffMs = 50
    val reconnectBackoffMsMs = 500
    val discoverBrokerVersions = true

    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      maxInflightRequestsPerConnection,
      reconnectBackoffMs,
      reconnectBackoffMsMs,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      config.quorumRequestTimeoutMs,
      config.connectionSetupTimeoutMs,
      config.connectionSetupTimeoutMaxMs,
      time,
      discoverBrokerVersions,
      apiVersions,
      logContext
    )
  }

  override def leaderAndEpoch: LeaderAndEpoch = {
    client.leaderAndEpoch
  }
}
