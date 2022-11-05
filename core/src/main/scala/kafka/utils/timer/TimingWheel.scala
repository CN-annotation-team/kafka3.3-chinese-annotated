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

import kafka.utils.nonthreadsafe

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

/*
 * Hierarchical Timing Wheels
 *
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), …,
 * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
 * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
 * insert/delete cost.
 *
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of n * u from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still O(1).
 *
 * Example
 * Let's say that u is 1 and n is 3. If the start time is c,
 * then the buckets at different levels are:
 *
 * level    buckets
 * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
 *
 * The bucket expiration is at the time of bucket beginning.
 * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired.
 * Level 1's clock moves to c+1, and [c+3,c+3] is created.
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
 * So, no new buckets are created in level 2 and 3.
 *
 * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 *
 * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 *
 * At time = c+2, [c+1,c+1] is newly expired.
 * Level 1 moves to c+2, and [c+4,c+4] is created,
 *
 * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 *
 * At time = c+3, [c+2,c+2] is newly expired.
 * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
 * Level 3 stay at c.
 *
 * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 *
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
 * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
 *
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 */

/**
 * 时间轮，时间轮是由底层向上层时间轮进行创建，初始化定时任务功能的时候，会创建一个最底层的时间轮，
 * 最底层的时间轮默认有 20(wheelSize) 格，每格的时间间隔是 1ms(tickMs).也就是只能管理 20ms
 * 当底层的时间
 * @param tickMs        表示当前时间轮中一个时间格的时间跨度
 * @param wheelSize     当前时间轮的格数
 * @param startMs       当前时间轮的开始时间
 * @param taskCounter   任务计数器
 * @param queue         整个层级时间轮共用的任务队列，元素是 timerTaskList
 */
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {

  /** 当前时间轮表示的时间总长度 */
  private[this] val interval = tickMs * wheelSize
  /** 存储 timerTaskList，每个 bucket 表示一个时间格，一个 timerTaskList 中的任务到期时间可能不同，但间隔在该时间格范围内 */
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }

  /** 时间轮的指针，将时间分为到期部分和未到期部分 */
  private[this] var currentTime = startMs - (startMs % tickMs) // rounding down to multiple of tickMs

  // overflowWheel can potentially be updated and read by two concurrent threads through add().
  // Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
  /** 上层时间轮的引用 */
  @volatile private[this] var overflowWheel: TimingWheel = null

  /** 创建上层时间轮 */
  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        overflowWheel = new TimingWheel(
          // 上层时间轮的tickMs是当前时间轮的整个时间范围
          tickMs = interval,
          // 格数都是一样
          wheelSize = wheelSize,
          // 开始时间使用当前时间轮的指针
          startMs = currentTime,
          // 任务计数器和队列所有时间轮共用
          taskCounter = taskCounter,
          queue
        )
      }
    }
  }

  /** 向时间轮添加定时任务 */
  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    // 获取定时任务的过期时间
    val expiration = timerTaskEntry.expirationMs

    if (timerTaskEntry.cancelled) {
      // 如果定时任务被标记为取消了，直接返回
      // Cancelled
      false
    } else if (expiration < currentTime + tickMs) {
      // 已经过期了，直接返回，这里不用担心已经过期的任务不被执行，该方法的调用出会对过期任务进行处理
      // Already expired
      false
    } else if (expiration < currentTime + interval) {
      // 过期时间小于当前时间轮的最大时间，放入到对应的 bucket 中
      // Put in its own bucket
      // 过期时间 / 每格时间间隔 % 当前时间轮格数 = 实际的桶位置
      val virtualId = expiration / tickMs
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      // 将定时任务添加到 bucket 中
      bucket.add(timerTaskEntry)

      // Set the bucket expiration time
      // 设置这个 bucket 的过期时间，使用 virtualId * tickMs 将时间规范化（其实就是将 expiration 做了 tickMs 向下对齐的效果），
      // 距离当前任务最近的且小于当前任务过期时间的点
      /**
       * 注意：这个地方是一个重点逻辑，判断是否这个桶要放入延迟队列中
       * 需要知道的前提条件，bucket 的 expiration 在初始化的时候会被设置为 -1，然后对 bucket 进行 flush 操作的时候，也会置为 -1
       * 其实就是当一个空桶加入一个元素，就会导致 expiration 改变，即会将这个桶放入延迟队列中
       */
      if (bucket.setExpiration(virtualId * tickMs)) {
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.
        queue.offer(bucket)
      }
      true
    } else {
      // Out of the interval. Put it into the parent timer
      // 超出了当前时间轮管控的范围，交给上层时间轮处理，没有上层时间轮，就创建上层时间轮
      if (overflowWheel == null) addOverflowWheel()
      overflowWheel.add(timerTaskEntry)
    }
  }

  // Try to advance the clock
  /** 推动当前时间轮的指针，如果有上层时间轮，也会推进上层时间轮的指针 */
  def advanceClock(timeMs: Long): Unit = {
    // 推动的时间至少要超过当前时间轮的一格时间间隔
    if (timeMs >= currentTime + tickMs) {
      // 向下取最近的时间点
      currentTime = timeMs - (timeMs % tickMs)

      // Try to advance the clock of the overflow wheel if present
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
}
