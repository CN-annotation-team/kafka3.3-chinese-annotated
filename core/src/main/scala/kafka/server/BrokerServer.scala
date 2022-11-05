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

import java.net.InetAddress
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit, TimeoutException}

import kafka.cluster.Broker.ServerInfo
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.{ProducerIdManager, TransactionCoordinator}
import kafka.log.LogManager
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.raft.RaftManager
import kafka.security.CredentialProvider
import kafka.server.KafkaRaftServer.ControllerRole
import kafka.server.metadata.BrokerServerMetrics
import kafka.server.metadata.{BrokerMetadataListener, BrokerMetadataPublisher, BrokerMetadataSnapshotter, ClientQuotaMetadataManager, KRaftMetadataCache, SnapshotWriterBuilder}
import kafka.utils.{CoreUtils, KafkaScheduler}
import org.apache.kafka.common.feature.SupportedVersionRange
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{Listener, ListenerCollection}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time, Utils}
import org.apache.kafka.common.{ClusterResource, Endpoint}
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer
import org.apache.kafka.metadata.{BrokerState, VersionRange}
import org.apache.kafka.raft.RaftConfig.AddressSpec
import org.apache.kafka.raft.{RaftClient, RaftConfig}
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.server.fault.FaultHandler
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.snapshot.SnapshotWriter

import scala.collection.{Map, Seq}
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._


class BrokerSnapshotWriterBuilder(raftClient: RaftClient[ApiMessageAndVersion])
    extends SnapshotWriterBuilder {
  override def build(committedOffset: Long,
                     committedEpoch: Int,
                     lastContainedLogTime: Long): Option[SnapshotWriter[ApiMessageAndVersion]] = {
    raftClient.createSnapshot(committedOffset, committedEpoch, lastContainedLogTime).asScala
  }
}

/**
 * A Kafka broker that runs in KRaft (Kafka Raft) mode.
 */
class BrokerServer(
  val config: KafkaConfig,
  val metaProps: MetaProperties,
  val raftManager: RaftManager[ApiMessageAndVersion],
  val time: Time,
  val metrics: Metrics,
  val brokerMetrics: BrokerServerMetrics,
  val threadNamePrefix: Option[String],
  val initialOfflineDirs: Seq[String],
  val controllerQuorumVotersFuture: CompletableFuture[util.Map[Integer, AddressSpec]],
  val fatalFaultHandler: FaultHandler,
  val metadataLoadingFaultHandler: FaultHandler,
  val metadataPublishingFaultHandler: FaultHandler
) extends KafkaBroker {

  override def brokerState: BrokerState = Option(lifecycleManager).
    flatMap(m => Some(m.state)).getOrElse(BrokerState.NOT_RUNNING)

  import kafka.server.Server._

  private val logContext: LogContext = new LogContext(s"[BrokerServer id=${config.nodeId}] ")

  this.logIdent = logContext.logPrefix

  @volatile var lifecycleManager: BrokerLifecycleManager = null

  private val isShuttingDown = new AtomicBoolean(false)

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  @volatile var dataPlaneRequestProcessor: KafkaApis = null
  var controlPlaneRequestProcessor: KafkaApis = null

  var authorizer: Option[Authorizer] = None
  @volatile var socketServer: SocketServer = null
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = null

  var logDirFailureChannel: LogDirFailureChannel = null
  // 日志管理器
  var logManager: LogManager = null

  var tokenManager: DelegationTokenManager = null

  var dynamicConfigHandlers: Map[String, ConfigHandler] = null

  /**
   * 1. 副本管理器，{@link ReplicaManager} 会负责 replica 的创建删除，信息修改，主从切换，
   * 2. 副本是分区在一个 broker 节点中的表现形式，replicaManager 可以认为它也是分区管理器，但他只会负责分区的创建删除
   * (可以看到下面对于分区还有一个 {@link AlterPartitionManager}，这个不叫 PartitionManager 就是因为创建删除分区
   * 不归它管理，它只负责修改)
   */
  @volatile private[this] var _replicaManager: ReplicaManager = null

  var credentialProvider: CredentialProvider = null
  var tokenCache: DelegationTokenCache = null

  // 消费者分组协调器
  @volatile var groupCoordinator: GroupCoordinator = null

  // 事务协调器
  var transactionCoordinator: TransactionCoordinator = null

  /** broker 角色和 controller 角色之间的连接，broker 方是客户端，这里其实就是管理一个 tcp 客户端连接 controller */
  var clientToControllerChannelManager: BrokerToControllerChannelManager = null

  /** 请求重定向管理器，部分请求需要进行重定向，这个功能其实主要是服务 kraft 模式下，部分请求需要让 controller leader
   * 节点来处理，然后 broker 会同步 controller leader 处理请求后生成的动作日志，根据日志的内容再通过 {@link BrokerMetadataListener}
   * 做具体的处理 */
  var forwardingManager: ForwardingManager = null

  // partition 修改管理器
  var alterPartitionManager: AlterPartitionManager = null

  // topic 自动创建管理器
  var autoTopicCreationManager: AutoTopicCreationManager = null

  // 线程池
  var kafkaScheduler: KafkaScheduler = null

  // 当前 broker 节点保存的 kraft 相关的元数据信息缓存
  @volatile var metadataCache: KRaftMetadataCache = null

  var quotaManagers: QuotaFactory.QuotaManagers = null

  var clientQuotaMetadataManager: ClientQuotaMetadataManager = null

  @volatile var brokerTopicStats: BrokerTopicStats = null

  val clusterId: String = metaProps.clusterId

  var metadataSnapshotter: Option[BrokerMetadataSnapshotter] = None

  /** 集群元数据监听器，监听 __cluster-metadata topic 日志，该实例会被注册到 KafkaRaftClient 中，
   * 当 kafkaRaftClient FETCH 到了新的日志会触发该监听器做对应的处理 */
  var metadataListener: BrokerMetadataListener = null

  /** 集群元数据事件发布者，上面的监听器监听到有新的日志到来，会根据日志记录的动作，来更新 {@link org.apache.kafka.image.MetadataImage}
   * 对应的部分，然后重新生成一个 metadata 镜像，这里的 metadataPublisher 则会将这个新的 image 进行解析
   * 将具体的更改分发给对应的管理器去执行 */
  var metadataPublisher: BrokerMetadataPublisher = null

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault()

  def kafkaYammerMetrics: KafkaYammerMetrics = KafkaYammerMetrics.INSTANCE

  // 状态机改变
  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      info(s"Transition from $status to $to")

      status = to
      if (to == SHUTTING_DOWN) {
        isShuttingDown.set(true)
      } else if (to == SHUTDOWN) {
        isShuttingDown.set(false)
        awaitShutdownCond.signalAll()
      }
    } finally {
      lock.unlock()
    }
    true
  }

  def replicaManager: ReplicaManager = _replicaManager

  override def startup(): Unit = {
    // 从 SHUTDOWN 状态转换成 STARTING 状态
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    try {
      info("Starting broker")

      config.dynamicConfig.initialize(zkClientOpt = None)

      // 初始化 broker 的生命周期管理器
      lifecycleManager = new BrokerLifecycleManager(config,
        time,
        threadNamePrefix)

      /* start scheduler */
      /* 根据配置指定的 background.threads 来创建一个线程池 */
      kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
      kafkaScheduler.startup()

      /* register broker metrics */
      brokerTopicStats = new BrokerTopicStats

      quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))

      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

      metadataCache = MetadataCache.kRaftMetadataCache(config.nodeId)

      // Create log manager, but don't start it because we need to delay any potential unclean shutdown log recovery
      // until we catch up on the metadata log and have up-to-date topic and broker configs.
      // 创建日志管理器，但是不会立即启动该日志管理器，需要等待上一次关闭时的日志信息恢复
      logManager = LogManager(config, initialOfflineDirs, metadataCache, kafkaScheduler, time,
        brokerTopicStats, logDirFailureChannel, keepPartitionMetadataFile = true)

      // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
      // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

      val controllerNodes = RaftConfig.voterConnectionsToNodes(controllerQuorumVotersFuture.get()).asScala
      val controllerNodeProvider = RaftControllerNodeProvider(raftManager, config, controllerNodes)

      // 管理当前 broker 和 集群 Controller 之间的连接
      clientToControllerChannelManager = BrokerToControllerChannelManager(
        controllerNodeProvider,
        time,
        metrics,
        config,
        channelName = "forwarding",
        threadNamePrefix,
        retryTimeoutMs = 60000
      )
      clientToControllerChannelManager.start()
      // 实例化重定向管理器
      forwardingManager = new ForwardingManagerImpl(clientToControllerChannelManager)


      val apiVersionManager = ApiVersionManager(
        ListenerType.BROKER,
        config,
        Some(forwardingManager),
        brokerFeatures,
        metadataCache
      )

      // Create and start the socket server acceptor threads so that the bound port is known.
      // Delay starting processors until the end of the initialization sequence to ensure
      // that credentials have been loaded before processing authentications.
      // 创建 socketServer
      socketServer = new SocketServer(config, metrics, time, credentialProvider, apiVersionManager)

      clientQuotaMetadataManager = new ClientQuotaMetadataManager(quotaManagers, socketServer.connectionQuotas)

      // partition 更新管理器
      alterPartitionManager = AlterPartitionManager(
        config,
        metadataCache,
        scheduler = kafkaScheduler,
        controllerNodeProvider,
        time = time,
        metrics,
        threadNamePrefix,
        brokerEpochSupplier = () => lifecycleManager.brokerEpoch
      )
      alterPartitionManager.start()

      // 副本管理器
      this._replicaManager = new ReplicaManager(
        config = config,
        metrics = metrics,
        time = time,
        scheduler = kafkaScheduler,
        logManager = logManager,
        quotaManagers = quotaManagers,
        metadataCache = metadataCache,
        logDirFailureChannel = logDirFailureChannel,
        alterPartitionManager = alterPartitionManager,
        brokerTopicStats = brokerTopicStats,
        isShuttingDown = isShuttingDown,
        zkClient = None,
        threadNamePrefix = threadNamePrefix)

      /* start token manager */
      if (config.tokenAuthEnabled) {
        throw new UnsupportedOperationException("Delegation tokens are not supported")
      }
      tokenManager = new DelegationTokenManager(config, tokenCache, time , null)
      tokenManager.startup() // does nothing, we just need a token manager in order to compile right now...

      // Create group coordinator, but don't start it until we've started replica manager.
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      // 消费者组协调器
      groupCoordinator = GroupCoordinator(config, replicaManager, Time.SYSTEM, metrics)

      val producerIdManagerSupplier = () => ProducerIdManager.rpc(
        config.brokerId,
        brokerEpochSupplier = () => lifecycleManager.brokerEpoch,
        clientToControllerChannelManager,
        config.requestTimeoutMs
      )

      // Create transaction coordinator, but don't start it until we've started replica manager.
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      // 事务协调器
      transactionCoordinator = TransactionCoordinator(config, replicaManager,
        new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-"),
        producerIdManagerSupplier, metrics, metadataCache, Time.SYSTEM)

      // 自动创建 topic 管理器
      autoTopicCreationManager = new DefaultAutoTopicCreationManager(
        config, Some(clientToControllerChannelManager), None, None,
        groupCoordinator, transactionCoordinator)

      /* Add all reconfigurables for config change notification before starting the metadata listener */
      config.dynamicConfig.addReconfigurables(this)

      dynamicConfigHandlers = Map[String, ConfigHandler](
        ConfigType.Topic -> new TopicConfigHandler(logManager, config, quotaManagers, None),
        ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers))

      if (!config.processRoles.contains(ControllerRole)) {
        // If no controller is defined, we rely on the broker to generate snapshots.
        metadataSnapshotter = Some(new BrokerMetadataSnapshotter(
          config.nodeId,
          time,
          threadNamePrefix,
          new BrokerSnapshotWriterBuilder(raftManager.client)
        ))
      }

      metadataListener = new BrokerMetadataListener(
        config.nodeId,
        time,
        threadNamePrefix,
        config.metadataSnapshotMaxNewRecordBytes,
        metadataSnapshotter,
        brokerMetrics,
        metadataLoadingFaultHandler)

      val networkListeners = new ListenerCollection()
      config.effectiveAdvertisedListeners.foreach { ep =>
        networkListeners.add(new Listener().
          setHost(if (Utils.isBlank(ep.host)) InetAddress.getLocalHost.getCanonicalHostName else ep.host).
          setName(ep.listenerName.value()).
          setPort(if (ep.port == 0) socketServer.boundPort(ep.listenerName) else ep.port).
          setSecurityProtocol(ep.securityProtocol.id))
      }

      val featuresRemapped = brokerFeatures.supportedFeatures.features().asScala.map {
        case (k: String, v: SupportedVersionRange) =>
          k -> VersionRange.of(v.min, v.max)
      }.asJava

      val brokerLifecycleChannelManager = BrokerToControllerChannelManager(
        controllerNodeProvider,
        time,
        metrics,
        config,
        "heartbeat",
        threadNamePrefix,
        config.brokerSessionTimeoutMs.toLong
      )
      lifecycleManager.start(
        () => metadataListener.highestMetadataOffset,
        brokerLifecycleChannelManager,
        metaProps.clusterId,
        networkListeners,
        featuresRemapped
      )

      // Register a listener with the Raft layer to receive metadata event notifications
      raftManager.register(metadataListener)

      val endpoints = new util.ArrayList[Endpoint](networkListeners.size())
      var interBrokerListener: Endpoint = null
      networkListeners.iterator().forEachRemaining(listener => {
        val endPoint = new Endpoint(listener.name(),
          SecurityProtocol.forId(listener.securityProtocol()),
          listener.host(), listener.port())
        endpoints.add(endPoint)
        if (listener.name().equals(config.interBrokerListenerName.value())) {
          interBrokerListener = endPoint
        }
      })
      if (interBrokerListener == null) {
        throw new RuntimeException("Unable to find inter-broker listener " +
          config.interBrokerListenerName.value() + ". Found listener(s): " +
          endpoints.asScala.map(ep => ep.listenerName().orElse("(none)")).mkString(", "))
      }
      val authorizerInfo = ServerInfo(new ClusterResource(clusterId),
        config.nodeId,
        endpoints,
        interBrokerListener,
        config.earlyStartListeners.map(_.value()).asJava)

      // Create and initialize an authorizer if one is configured.
      authorizer = config.createNewAuthorizer()
      authorizer.foreach(_.configure(config.originals))
      val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
        case Some(authZ) =>
          authZ.start(authorizerInfo).asScala.map { case (ep, cs) =>
            ep -> cs.toCompletableFuture
          }
        case None =>
          authorizerInfo.endpoints.asScala.map { ep =>
            ep -> CompletableFuture.completedFuture[Void](null)
          }.toMap
      }

      val fetchManager = new FetchManager(Time.SYSTEM,
        new FetchSessionCache(config.maxIncrementalFetchSessionCacheSlots,
          KafkaServer.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS))

      // Create the request processor objects.
      // 创建 raft 元数据管理器
      val raftSupport = RaftSupport(forwardingManager, metadataCache)
      // broker 角色开放的 apis
      dataPlaneRequestProcessor = new KafkaApis(
        requestChannel = socketServer.dataPlaneRequestChannel,
        metadataSupport = raftSupport,
        replicaManager = replicaManager,
        groupCoordinator = groupCoordinator,
        txnCoordinator = transactionCoordinator,
        autoTopicCreationManager = autoTopicCreationManager,
        brokerId = config.nodeId,
        config = config,
        configRepository = metadataCache,
        metadataCache = metadataCache,
        metrics = metrics,
        authorizer = authorizer,
        quotas = quotaManagers,
        fetchManager = fetchManager,
        brokerTopicStats = brokerTopicStats,
        clusterId = clusterId,
        time = time,
        tokenManager = tokenManager,
        apiVersionManager = apiVersionManager)

      // 请求处理线程池
      dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.nodeId,
        socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
        config.numIoThreads, s"${DataPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent",
        DataPlaneAcceptor.ThreadPrefix)

      // Block until we've caught up with the latest metadata from the controller quorum.
      lifecycleManager.initialCatchUpFuture.get()

      // Apply the metadata log changes that we've accumulated.
      metadataPublisher = new BrokerMetadataPublisher(config,
        metadataCache,
        logManager,
        replicaManager,
        groupCoordinator,
        transactionCoordinator,
        clientQuotaMetadataManager,
        dynamicConfigHandlers.toMap,
        authorizer,
        fatalFaultHandler,
        metadataPublishingFaultHandler)

      // Tell the metadata listener to start publishing its output, and wait for the first
      // publish operation to complete. This first operation will initialize logManager,
      // replicaManager, groupCoordinator, and txnCoordinator. The log manager may perform
      // a potentially lengthy recovery-from-unclean-shutdown operation here, if required.
      try {
        metadataListener.startPublishing(metadataPublisher).get()
      } catch {
        case t: Throwable => throw new RuntimeException("Received a fatal error while " +
          "waiting for the broker to catch up with the current cluster metadata.", t)
      }

      // Log static broker configurations.
      new KafkaConfig(config.originals(), true)

      // Enable inbound TCP connections. Each endpoint will be started only once its matching
      // authorizer future is completed.
      socketServer.enableRequestProcessing(authorizerFutures)

      // If we are using a ClusterMetadataAuthorizer which stores its ACLs in the metadata log,
      // notify it that the loading process is complete.
      authorizer match {
        case Some(clusterMetadataAuthorizer: ClusterMetadataAuthorizer) =>
          clusterMetadataAuthorizer.completeInitialLoad()
        case _ => // nothing to do
      }

      // We're now ready to unfence the broker. This also allows this broker to transition
      // from RECOVERY state to RUNNING state, once the controller unfences the broker.
      try {
        lifecycleManager.setReadyToUnfence().get()
      } catch {
        case t: Throwable => throw new RuntimeException("Received a fatal error while " +
          "waiting for the broker to be unfenced.", t)
      }

      maybeChangeStatus(STARTING, STARTED)
    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during broker startup. Prepare to shutdown", e)
        shutdown()
        throw if (e.isInstanceOf[ExecutionException]) e.getCause else e
    }
  }

  override def shutdown(): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      info("shutting down")

      if (config.controlledShutdownEnable) {
        if (replicaManager != null)
          replicaManager.beginControlledShutdown()

        lifecycleManager.beginControlledShutdown()
        try {
          lifecycleManager.controlledShutdownFuture.get(5L, TimeUnit.MINUTES)
        } catch {
          case _: TimeoutException =>
            error("Timed out waiting for the controller to approve controlled shutdown")
          case e: Throwable =>
            error("Got unexpected exception waiting for controlled shutdown future", e)
        }
      }

      if (metadataListener != null)
        metadataListener.beginShutdown()

      lifecycleManager.beginShutdown()

      // Stop socket server to stop accepting any more connections and requests.
      // Socket server will be shutdown towards the end of the sequence.
      if (socketServer != null) {
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      }
      if (dataPlaneRequestHandlerPool != null)
        CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
      if (dataPlaneRequestProcessor != null)
        CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
      if (controlPlaneRequestProcessor != null)
        CoreUtils.swallow(controlPlaneRequestProcessor.close(), this)
      CoreUtils.swallow(authorizer.foreach(_.close()), this)
      if (metadataListener != null) {
        CoreUtils.swallow(metadataListener.close(), this)
      }
      metadataSnapshotter.foreach(snapshotter => CoreUtils.swallow(snapshotter.close(), this))

      /**
       * We must shutdown the scheduler early because otherwise, the scheduler could touch other
       * resources that might have been shutdown and cause exceptions.
       * For example, if we didn't shutdown the scheduler first, when LogManager was closing
       * partitions one by one, the scheduler might concurrently delete old segments due to
       * retention. However, the old segments could have been closed by the LogManager, which would
       * cause an IOException and subsequently mark logdir as offline. As a result, the broker would
       * not flush the remaining partitions or write the clean shutdown marker. Ultimately, the
       * broker would have to take hours to recover the log during restart.
       */
      if (kafkaScheduler != null)
        CoreUtils.swallow(kafkaScheduler.shutdown(), this)

      if (transactionCoordinator != null)
        CoreUtils.swallow(transactionCoordinator.shutdown(), this)
      if (groupCoordinator != null)
        CoreUtils.swallow(groupCoordinator.shutdown(), this)

      if (tokenManager != null)
        CoreUtils.swallow(tokenManager.shutdown(), this)

      if (replicaManager != null)
        CoreUtils.swallow(replicaManager.shutdown(), this)

      if (alterPartitionManager != null)
        CoreUtils.swallow(alterPartitionManager.shutdown(), this)

      if (clientToControllerChannelManager != null)
        CoreUtils.swallow(clientToControllerChannelManager.shutdown(), this)

      if (logManager != null)
        CoreUtils.swallow(logManager.shutdown(), this)

      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)

      if (socketServer != null)
        CoreUtils.swallow(socketServer.shutdown(), this)
      if (metrics != null)
        CoreUtils.swallow(metrics.close(), this)
      if (brokerTopicStats != null)
        CoreUtils.swallow(brokerTopicStats.close(), this)

      // Clear all reconfigurable instances stored in DynamicBrokerConfig
      config.dynamicConfig.clear()

      isShuttingDown.set(false)

      CoreUtils.swallow(lifecycleManager.close(), this)

      CoreUtils.swallow(AppInfoParser.unregisterAppInfo(MetricsPrefix, config.nodeId.toString, metrics), this)
      info("shut down completed")
    } catch {
      case e: Throwable =>
        fatal("Fatal error during broker shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  override def awaitShutdown(): Unit = {
    lock.lock()
    try {
      while (true) {
        if (status == SHUTDOWN) return
        awaitShutdownCond.awaitUninterruptibly()
      }
    } finally {
      lock.unlock()
    }
  }

  override def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

}
