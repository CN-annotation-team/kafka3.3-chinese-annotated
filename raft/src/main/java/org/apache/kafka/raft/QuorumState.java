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
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is responsible for managing the current state of this node and ensuring
 * only valid state transitions. Below we define the possible state transitions and
 * how they are triggered:
 *
 * Unattached|Resigned transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Voted: After granting a vote to a candidate
 *    Candidate: After expiration of the election timeout
 *    Follower: After discovering a leader with an equal or larger epoch
 *
 * Voted transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Candidate: After expiration of the election timeout
 *
 * Candidate transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Candidate: After expiration of the election timeout
 *    Leader: After receiving a majority of votes
 *
 * Leader transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Resigned: When shutting down gracefully
 *
 * Follower transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Candidate: After expiration of the fetch timeout
 *    Follower: After discovering a leader with a larger epoch
 *
 * Observers follow a simpler state machine. The Voted/Candidate/Leader/Resigned
 * states are not possible for observers, so the only transitions that are possible
 * are between Unattached and Follower.
 *
 * Unattached transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Follower: After discovering a leader with an equal or larger epoch
 *
 * Follower transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Follower: After discovering a leader with a larger epoch
 *
 */
public class QuorumState {
    private final OptionalInt localId;
    private final Time time;
    private final Logger log;
    // quorum 状态信息在内存和磁盘之间转换的工具类
    private final QuorumStateStore store;
    // 当前配置文件中 controller.quorum.voters 配置的投票者集合
    private final Set<Integer> voters;
    private final Random random;
    // 选举超时时间，默认是 1000ms，该字段会用来节点发起投票的时间
    private final int electionTimeoutMs;
    private final int fetchTimeoutMs;
    private final LogContext logContext;

    private volatile EpochState state;

    public QuorumState(OptionalInt localId,
                       Set<Integer> voters,
                       int electionTimeoutMs,
                       int fetchTimeoutMs,
                       QuorumStateStore store,
                       Time time,
                       LogContext logContext,
                       Random random) {
        this.localId = localId;
        this.voters = new HashSet<>(voters);
        this.electionTimeoutMs = electionTimeoutMs;
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.store = store;
        this.time = time;
        this.log = logContext.logger(QuorumState.class);
        this.random = random;
        this.logContext = logContext;
    }

    public void initialize(OffsetAndEpoch logEndOffsetAndEpoch) throws IllegalStateException {
        // We initialize in whatever state we were in on shutdown. If we were a leader
        // or candidate, probably an election was held, but we will find out about it
        // when we send Vote or BeginEpoch requests.

        ElectionState election;
        try {
            /** 读取本地的存储的选举状态，具体文件的内容格式可以看 {@link FileBasedStateStore} */
            election = store.readElectionState();
            // 如果本地有存储选举状态，即之前已经选举过，但是节点挂掉了，直接基于上一次存储状态开始选举
            // 如果是第一次选举，设置纪元为 0，与所有参数投票的角色生成一个 ElectionState 实例
            if (election == null) {
                election = ElectionState.withUnknownLeader(0, voters);
            }
        } catch (final UncheckedIOException e) {
            // For exceptions during state file loading (missing or not readable),
            // we could assume the file is corrupted already and should be cleaned up.
            log.warn("Clearing local quorum state store after error loading state {}",
                store.toString(), e);
            store.clear();
            election = ElectionState.withUnknownLeader(0, voters);
        }

        final EpochState initialState;
        // 当前配置文件设置的的投票者和之前的投票者不吻合，报错
        if (!election.voters().isEmpty() && !voters.equals(election.voters())) {
            throw new IllegalStateException("Configured voter set: " + voters
                + " is different from the voter set read from the state file: " + election.voters()
                + ". Check if the quorum configuration is up to date, "
                + "or wipe out the local state file if necessary");
        } else if (election.hasVoted() && !isVoter()) {
            // 如果之前的配置的投票 ID 存在，但是当前节点不在投票者集合中，报错
            String localIdDescription = localId.isPresent() ?
                localId.getAsInt() + " is not a voter" :
                "is undefined";
            throw new IllegalStateException("Initialized quorum state " + election
                + " with a voted candidate, which indicates this node was previously "
                + " a voter, but the local id " + localIdDescription);
        } else if (election.epoch < logEndOffsetAndEpoch.epoch) {
            log.warn("Epoch from quorum-state file is {}, which is " +
                "smaller than last written epoch {} in the log",
                election.epoch, logEndOffsetAndEpoch.epoch);
            // 如果之前保留的选举纪元 < 当前纪元，则初始化状态为未连接状态，可以看做该角色挂掉了，在集群运行一段时间后，该节点有上线了
            initialState = new UnattachedState(
                time,
                // 纪元设置为当前纪元
                logEndOffsetAndEpoch.epoch,
                voters,
                // 由于掉线了一段时间，所以不知道高水标是多少
                Optional.empty(),
                // 生成随机选举超时时间
                randomElectionTimeoutMs(),
                logContext
            );
        } else if (localId.isPresent() && election.isLeader(localId.getAsInt())) {
            // If we were previously a leader, then we will start out as resigned
            // in the same epoch. This serves two purposes:
            // 1. It ensures that we cannot vote for another leader in the same epoch.
            // 2. It protects the invariant that each record is uniquely identified by
            //    offset and epoch, which might otherwise be violated if unflushed data
            //    is lost after restarting.
            // 如果之前是 leader 节点，且到这里已经排除了纪元落后的情况，将初始化状态设置为辞职状态
            initialState = new ResignedState(
                time,
                localId.getAsInt(),
                election.epoch,
                voters,
                randomElectionTimeoutMs(),
                Collections.emptyList(),
                logContext
            );
        } else if (localId.isPresent() && election.isVotedCandidate(localId.getAsInt())) {
            // 如果当前节点投票的节点 ID 就是当前节点，那么当前节点初始状态为获选人状态
            initialState = new CandidateState(
                time,
                localId.getAsInt(),
                election.epoch,
                voters,
                Optional.empty(),
                1,
                randomElectionTimeoutMs(),
                logContext
            );
        } else if (election.hasVoted()) {
            // 如果当前节点已经投了票，则初始状态为 已投票状态
            initialState = new VotedState(
                time,
                election.epoch,
                election.votedId(),
                voters,
                Optional.empty(),
                randomElectionTimeoutMs(),
                logContext
            );
        } else if (election.hasLeader()) {
            // 如果之前的 quorum 信息有 leader id，则当前节点的状态为 follower
            // 上面已经判断过 leader id 为 当前节点的情况了
            initialState = new FollowerState(
                time,
                election.epoch,
                election.leaderId(),
                voters,
                Optional.empty(),
                fetchTimeoutMs,
                logContext
            );
        } else {
            // 其他的情况都认为是离线状态
            initialState = new UnattachedState(
                time,
                election.epoch,
                voters,
                Optional.empty(),
                randomElectionTimeoutMs(),
                logContext
            );
        }

        // 转换当前节点的初始状态
        transitionTo(initialState);
    }

    public Set<Integer> remoteVoters() {
        return voters.stream().filter(voterId -> voterId != localIdOrSentinel()).collect(Collectors.toSet());
    }

    public int localIdOrSentinel() {
        return localId.orElse(-1);
    }

    public int localIdOrThrow() {
        return localId.orElseThrow(() -> new IllegalStateException("Required local id is not present"));
    }

    public OptionalInt localId() {
        return localId;
    }

    public int epoch() {
        return state.epoch();
    }

    public int leaderIdOrSentinel() {
        return leaderId().orElse(-1);
    }

    public Optional<LogOffsetMetadata> highWatermark() {
        return state.highWatermark();
    }

    public OptionalInt leaderId() {

        ElectionState election = state.election();
        if (election.hasLeader())
            return OptionalInt.of(state.election().leaderId());
        else
            return OptionalInt.empty();
    }

    public boolean hasLeader() {
        return leaderId().isPresent();
    }

    public boolean hasRemoteLeader() {
        return hasLeader() && leaderIdOrSentinel() != localIdOrSentinel();
    }

    // 是否是投票者，就是判断当前节点 ID 存在，且存在配置文件配置的投票者集合中
    public boolean isVoter() {
        return localId.isPresent() && voters.contains(localId.getAsInt());
    }

    public boolean isVoter(int nodeId) {
        return voters.contains(nodeId);
    }

    public boolean isObserver() {
        return !isVoter();
    }

    public void transitionToResigned(List<Integer> preferredSuccessors) {
        if (!isLeader()) {
            throw new IllegalStateException("Invalid transition to Resigned state from " + state);
        }

        // The Resigned state is a soft state which does not need to be persisted.
        // A leader will always be re-initialized in this state.
        int epoch = state.epoch();
        this.state = new ResignedState(
            time,
            localIdOrThrow(),
            epoch,
            voters,
            randomElectionTimeoutMs(),
            preferredSuccessors,
            logContext
        );
        log.info("Completed transition to {}", state);
    }

    /**
     * Transition to the "unattached" state. This means we have found an epoch greater than
     * or equal to the current epoch, but wo do not yet know of the elected leader.
     */
    public void transitionToUnattached(int epoch) {
        int currentEpoch = state.epoch();
        if (epoch <= currentEpoch) {
            throw new IllegalStateException("Cannot transition to Unattached with epoch= " + epoch +
                " from current state " + state);
        }

        final long electionTimeoutMs;
        if (isObserver()) {
            electionTimeoutMs = Long.MAX_VALUE;
        } else if (isCandidate()) {
            electionTimeoutMs = candidateStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else if (isVoted()) {
            electionTimeoutMs = votedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else if (isUnattached()) {
            electionTimeoutMs = unattachedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else {
            electionTimeoutMs = randomElectionTimeoutMs();
        }

        transitionTo(new UnattachedState(
            time,
            epoch,
            voters,
            state.highWatermark(),
            electionTimeoutMs,
            logContext
        ));
    }

    /**
     * Grant a vote to a candidate and become a follower for this epoch. We will remain in this
     * state until either the election timeout expires or a leader is elected. In particular,
     * we do not begin fetching until the election has concluded and {@link #transitionToFollower(int, int)}
     * is invoked.
     */
    public void transitionToVoted(
        int epoch,
        int candidateId
    ) {
        if (localId.isPresent() && candidateId == localId.getAsInt()) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " since it matches the local broker.id");
        } else if (isObserver()) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " since the local broker.id=" + localId + " is not a voter");
        } else if (!isVoter(candidateId)) {
            throw new IllegalStateException("Cannot transition to Voted with voterId=" + candidateId +
                " and epoch=" + epoch + " since it is not one of the voters " + voters);
        }

        int currentEpoch = state.epoch();
        if (epoch < currentEpoch) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " since the current epoch " + currentEpoch + " is larger");
        } else if (epoch == currentEpoch && !isUnattached()) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " from the current state " + state);
        }

        // Note that we reset the election timeout after voting for a candidate because we
        // know that the candidate has at least as good of a chance of getting elected as us

        transitionTo(new VotedState(
            time,
            epoch,
            candidateId,
            voters,
            state.highWatermark(),
            randomElectionTimeoutMs(),
            logContext
        ));
    }

    /**
     * Become a follower of an elected leader so that we can begin fetching.
     */
    public void transitionToFollower(
        int epoch,
        int leaderId
    ) {
        if (localId.isPresent() && leaderId == localId.getAsInt()) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " since it matches the local broker.id=" + localId);
        } else if (!isVoter(leaderId)) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " since it is not one of the voters " + voters);
        }

        int currentEpoch = state.epoch();
        if (epoch < currentEpoch) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " since the current epoch " + currentEpoch + " is larger");
        } else if (epoch == currentEpoch
            && (isFollower() || isLeader())) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " from state " + state);
        }

        transitionTo(new FollowerState(
            time,
            epoch,
            leaderId,
            voters,
            state.highWatermark(),
            fetchTimeoutMs,
            logContext
        ));
    }

    // 转换成候选者节点
    public void transitionToCandidate() {
        if (isObserver()) {
            throw new IllegalStateException("Cannot transition to Candidate since the local broker.id=" + localId +
                " is not one of the voters " + voters);
        } else if (isLeader()) {
            throw new IllegalStateException("Cannot transition to Candidate since the local broker.id=" + localId +
                " since this node is already a Leader with state " + state);
        }

        int retries = isCandidate() ? candidateStateOrThrow().retries() + 1 : 1;
        // 纪元 + 1
        int newEpoch = epoch() + 1;
        // 获取随机超时时间
        int electionTimeoutMs = randomElectionTimeoutMs();

        transitionTo(new CandidateState(
            time,
            localIdOrThrow(),
            newEpoch,
            voters,
            state.highWatermark(),
            retries,
            electionTimeoutMs,
            logContext
        ));
    }

    public <T> LeaderState<T> transitionToLeader(long epochStartOffset, BatchAccumulator<T> accumulator) {
        if (isObserver()) {
            throw new IllegalStateException("Cannot transition to Leader since the local broker.id="  + localId +
                " is not one of the voters " + voters);
        } else if (!isCandidate()) {
            throw new IllegalStateException("Cannot transition to Leader from current state " + state);
        }

        CandidateState candidateState = candidateStateOrThrow();
        if (!candidateState.isVoteGranted())
            throw new IllegalStateException("Cannot become leader without majority votes granted");

        // Note that the leader does not retain the high watermark that was known
        // in the previous state. The reason for this is to protect the monotonicity
        // of the global high watermark, which is exposed through the leader. The
        // only way a new leader can be sure that the high watermark is increasing
        // monotonically is to wait until a majority of the voters have reached the
        // starting offset of the new epoch. The downside of this is that the local
        // state machine is temporarily stalled by the advancement of the global
        // high watermark even though it only depends on local monotonicity. We
        // could address this problem by decoupling the local high watermark, but
        // we typically expect the state machine to be caught up anyway.

        LeaderState<T> state = new LeaderState<>(
            localIdOrThrow(),
            epoch(),
            epochStartOffset,
            voters,
            candidateState.grantingVoters(),
            accumulator,
            logContext
        );
        transitionTo(state);
        return state;
    }

    private void transitionTo(EpochState state) {
        if (this.state != null) {
            try {
                this.state.close();
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Failed to transition from " + this.state.name() + " to " + state.name(), e);
            }
        }

        this.store.writeElectionState(state.election());
        this.state = state;
        log.info("Completed transition to {}", state);
    }

    /**
     * raft 算法要求各个节点有不同的发起选举投票的时间，为了快速选出 leader 节点
     * 随机一个选举超时时间介于 electionTimeoutMs - 2 * electionTimeoutMs 之间
     * @return
     */
    private int randomElectionTimeoutMs() {
        if (electionTimeoutMs == 0)
            return 0;
        return electionTimeoutMs + random.nextInt(electionTimeoutMs);
    }

    public boolean canGrantVote(int candidateId, boolean isLogUpToDate) {
        return state.canGrantVote(candidateId, isLogUpToDate);
    }

    public FollowerState followerStateOrThrow() {
        if (isFollower())
            return (FollowerState) state;
        throw new IllegalStateException("Expected to be Follower, but the current state is " + state);
    }

    public VotedState votedStateOrThrow() {
        if (isVoted())
            return (VotedState) state;
        throw new IllegalStateException("Expected to be Voted, but current state is " + state);
    }

    public UnattachedState unattachedStateOrThrow() {
        if (isUnattached())
            return (UnattachedState) state;
        throw new IllegalStateException("Expected to be Unattached, but current state is " + state);
    }

    @SuppressWarnings("unchecked")
    public <T> LeaderState<T> leaderStateOrThrow() {
        if (isLeader())
            return (LeaderState<T>) state;
        throw new IllegalStateException("Expected to be Leader, but current state is " + state);
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<LeaderState<T>> maybeLeaderState() {
        EpochState state = this.state;
        if (state instanceof  LeaderState) {
            return Optional.of((LeaderState<T>) state);
        } else {
            return Optional.empty();
        }
    }

    public ResignedState resignedStateOrThrow() {
        if (isResigned())
            return (ResignedState) state;
        throw new IllegalStateException("Expected to be Resigned, but current state is " + state);
    }

    public CandidateState candidateStateOrThrow() {
        if (isCandidate())
            return (CandidateState) state;
        throw new IllegalStateException("Expected to be Candidate, but current state is " + state);
    }

    public LeaderAndEpoch leaderAndEpoch() {
        ElectionState election = state.election();
        return new LeaderAndEpoch(election.leaderIdOpt, election.epoch);
    }

    public boolean isFollower() {
        return state instanceof FollowerState;
    }

    public boolean isVoted() {
        return state instanceof VotedState;
    }

    public boolean isUnattached() {
        return state instanceof UnattachedState;
    }

    public boolean isLeader() {
        return state instanceof LeaderState;
    }

    public boolean isResigned() {
        return state instanceof ResignedState;
    }

    public boolean isCandidate() {
        return state instanceof CandidateState;
    }

}
