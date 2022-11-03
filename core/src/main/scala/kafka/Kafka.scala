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

package kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.server.{KafkaConfig, KafkaRaftServer, KafkaServer, Server}
import kafka.utils.Implicits._
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.common.utils.{Java, LoggingSignalHandler, OperatingSystem, Time, Utils}

import scala.jdk.CollectionConverters._

object Kafka extends Logging {

  def getPropsFromArgs(args: Array[String]): Properties = {
    val optionParser = new OptionParser(false)
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])
    // This is just to make the parameter show up in the help output, we are not actually using this due the
    // fact that this class ignores the first parameter which is interpreted as positional and mandatory
    // but would not be mandatory if --version is specified
    // This is a bit of an ugly crutch till we get a chance to rework the entire command line parsing
    optionParser.accepts("version", "Print version information and exit.")

    // 传入的参数是 --help 的情况，打印 help 信息，然后退出
    if (args.length == 0 || args.contains("--help")) {
      CommandLineUtils.printUsageAndDie(optionParser,
        "USAGE: java [options] %s server.properties [--override property=value]*".format(this.getClass.getCanonicalName.split('$').head))
    }

    // 传入的参数是 --version 的情况，答应 version 信息，然后退出
    if (args.contains("--version")) {
      CommandLineUtils.printVersionAndDie()
    }

    // 到这里，表示第一个参数是配置文件路径，将其加载成一个 Properties 实例
    val props = Utils.loadProps(args(0))

    if (args.length > 1) {
      val options = optionParser.parse(args.slice(1, args.length): _*)

      if (options.nonOptionArguments().size() > 0) {
        CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }

      props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala)
    }
    props
  }
  /* 构建 kafka 服务，会选择基于 zookeeper 来运行还是基于 kafka 自带的 kraft 来运行 */
  private def buildServer(props: Properties): Server = {
    // 这里会根据配置文件的配置和默认配置生成一个 KafkaConfig 实例
    val config = KafkaConfig.fromProps(props, false)
    // requiresZookeeper 其实就是判断配置文件中是否有配置 process.roles
    // 如果配置了表示不需要 zookeeper
    if (config.requiresZookeeper) {
      // 需要zookeeper的情况会实例化一个 KafkaServer 实例
      new KafkaServer(
        config,
        Time.SYSTEM,
        threadNamePrefix = None,
        enableForwarding = false
      )
    } else {
      // 使用 kraft 的情况会实例化一个 KafkaRaftServer 实例
      new KafkaRaftServer(
        config,
        Time.SYSTEM,
        threadNamePrefix = None
      )
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      // 会根据参数给定的配置文件 config/kraft/server.properties，将该文件解析成一个 Properties 实例
      val serverProps = getPropsFromArgs(args)
      // 如果是 kraft 模式启动，会实例化一个 KafkaRaftServer
      val server = buildServer(serverProps)

      try {
        if (!OperatingSystem.IS_WINDOWS && !Java.isIbmJdk)
          new LoggingSignalHandler().register()
      } catch {
        case e: ReflectiveOperationException =>
          warn("Failed to register optional signal handler that logs a message when the process is terminated " +
            s"by a signal. Reason for registration failure is: $e", e)
      }

      // attach shutdown handler to catch terminating signals as well as normal termination
      Exit.addShutdownHook("kafka-shutdown-hook", {
        try server.shutdown()
        catch {
          case _: Throwable =>
            fatal("Halting Kafka.")
            // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
            Exit.halt(1)
        }
      })

      // 启动服务
      try server.startup()
      catch {
        case e: Throwable =>
          // KafkaServer.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code
          fatal("Exiting Kafka due to fatal exception during startup.", e)
          Exit.exit(1)
      }

      // 这里会进行阻塞，等待关闭
      server.awaitShutdown()
    }
    catch {
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception", e)
        Exit.exit(1)
    }
    Exit.exit(0)
  }
}
