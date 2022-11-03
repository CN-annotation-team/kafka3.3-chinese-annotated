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
package org.apache.kafka.raft;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * A voter is "unattached" when it learns of an ongoing election (typically
 * by observing a bumped epoch), but has yet to cast its vote or become a
 * candidate itself.
 */
// 掉线状态
public class UnattachedState implements EpochState {
    // 当前纪元
    private final int epoch;
    // 所有投票者
    private final Set<Integer> voters;
    // 选举超时时间，该时间表示当前节点需要等待多久之后开始投票，每个节点的该事件都不同
    private final long electionTimeoutMs;
    // 选举定时器
    private final Timer electionTimer;
    private final Optional<LogOffsetMetadata> highWatermark;
    private final Logger log;

    public UnattachedState(
        Time time,
        int epoch,
        Set<Integer> voters,
        Optional<LogOffsetMetadata> highWatermark,
        long electionTimeoutMs,
        LogContext logContext
    ) {
        this.epoch = epoch;
        this.voters = voters;
        this.highWatermark = highWatermark;
        this.electionTimeoutMs = electionTimeoutMs;
        /** 这里需要注意一下，选举定时器设置了自己的 deadlineMs 为 当前时间戳 + 选举超时时间，在{@link KafkaRaftClient#pollUnattached(long)}
         * 方法中会判断当前kafka时间戳是否大于这个选举超时时间来判断是否开始进行 unattached 的逻辑处理 */
        this.electionTimer = time.timer(electionTimeoutMs);
        this.log = logContext.logger(UnattachedState.class);
    }

    /**
     * 该状态下的节点和集群脱节了，所以他不知道leaderId,也不能直接当作投票者，所以初始选举化状态时，第二三个参数是空
     * @return
     */
    @Override
    public ElectionState election() {
        return new ElectionState(
            epoch,
            OptionalInt.empty(),
            OptionalInt.empty(),
            voters
        );
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public String name() {
        return "Unattached";
    }

    public long electionTimeoutMs() {
        return electionTimeoutMs;
    }

    public long remainingElectionTimeMs(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.remainingMs();
    }

    public boolean hasElectionTimeoutExpired(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.isExpired();
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public boolean canGrantVote(int candidateId, boolean isLogUpToDate) {
        if (!isLogUpToDate) {
            log.debug("Rejecting vote request from candidate {} since candidate epoch/offset is not up to date with us",
                candidateId);
        }
        return isLogUpToDate;
    }

    @Override
    public String toString() {
        return "Unattached(" +
            "epoch=" + epoch +
            ", voters=" + voters +
            ", electionTimeoutMs=" + electionTimeoutMs +
            ')';
    }

    @Override
    public void close() {}
}
