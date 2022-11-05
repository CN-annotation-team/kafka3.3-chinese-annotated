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
package kafka.utils.timer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{DelayQueue, Executors, TimeUnit}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

// 定时器接口，提供了四个方法接口
trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    */
  // 添加一个新的定时任务
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  def shutdown(): Unit
}

// Kafka 定时器功能实现
@threadsafe
class SystemTimer(executorName: String,
                 // 最底层实践论的每个时间间隔 tickMs，默认 1ms
                  tickMs: Long = 1,
                 // 所有时间轮的格数
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  // timeout timer
  // 创建一个固定线程数线程池，线程数为 1
  private[this] val taskExecutor = Executors.newFixedThreadPool(1,
    (runnable: Runnable) => KafkaThread.nonDaemon("executor-" + executorName, runnable))

  // 阻塞推进时间轮表针的线程，等待最近任务到期
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  // 各个层级公用的任务计数器
  private[this] val taskCounter = new AtomicInteger(0)
  // 创建层级时间轮中最底层的时间轮，可以看出时间轮是从底层往上层创建
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  // 添加一个定时任务，将 TimerTask 封装成 TimerTaskEntry，并计算过期时间
  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  // 将任务添加到事件轮中
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      // 如果任务已经到期，但是没有被标记为取消，则将任务交给线程池执行
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    // 取出一个桶，这里需要了解 DelayQueue 的机制，即如果有元素的 delay 延迟时间到点了，则会将元素设置为头节点，
    // poll 方法就是将头节点拿出来。
    // 这里就是拿头节点，如果头节点不存在，则等待 timeoutMs 的时间再次拿头结点，如果还是不存在则返回 null
    // bucket 设置过期时间设置可以看 TimingWheel 的 add 方法，其实就是 bucket 所位于的时间格的起始时间
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        // 这里会将 delayQueue 中达到 delay 延迟时间的 bucket 都取出来
        while (bucket != null) {
          // 推进时间轮的表针到 bucket 所在时间格的起始时间
          timingWheel.advanceClock(bucket.getExpiration)
          // 这里是将上一层时间轮的任务重新分配给底层时间轮
          bucket.flush(addTimerTaskEntry)
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown(): Unit = {
    taskExecutor.shutdown()
  }

}
