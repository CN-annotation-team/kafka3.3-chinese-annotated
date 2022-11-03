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

/**
 * 封装节点的日志偏移量和纪元，该类主要是用于在进行节点选举投票的时候进行比较，该类的主要方法是 {@link #compareTo(OffsetAndEpoch)}
 */
public class OffsetAndEpoch implements Comparable<OffsetAndEpoch> {
    // 日志偏移量
    public final long offset;
    // 纪元
    public final int epoch;

    public OffsetAndEpoch(long offset, int epoch) {
        this.offset = offset;
        this.epoch = epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OffsetAndEpoch that = (OffsetAndEpoch) o;

        if (offset != that.offset) return false;
        return epoch == that.epoch;
    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + epoch;
        return result;
    }

    @Override
    public String toString() {
        return "OffsetAndEpoch(" +
                "offset=" + offset +
                ", epoch=" + epoch +
                ')';
    }

    /**
     * 用于比较两个节点之间，谁更有资格当 leader 节点
     * epoch 越大越有资格成为 leader 副本
     * epoch 相同，日志偏移量越大的越有资格成为 leader 副本
     * @param o
     * @return
     */
    @Override
    public int compareTo(OffsetAndEpoch o) {
        // 先比较 epoch
        if (epoch == o.epoch)
            // 再比较分区副本之间的日志偏移量
            return Long.compare(offset, o.offset);
        return Integer.compare(epoch, o.epoch);
    }
}
