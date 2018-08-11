/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.protocols.raft.protocol;

import io.atomix.cluster.MemberId;
import io.atomix.utils.misc.ArraySizeHashPrinter;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Server snapshot installation request.
 * <p>
 * Snapshot installation requests are sent by the leader to a follower when the follower indicates that its log is
 * further behind than the last snapshot taken by the leader. Snapshots are sent in chunks, with each chunk being sent
 * in a separate install request. As requests are received by the follower, the snapshot is reconstructed based on the
 * provided {@link #chunkOffset()} and other metadata. The last install request will be sent with {@link #complete()}
 * being {@code true} to indicate that all chunks of the snapshot have been sent.
 */
public class InstallRequest extends AbstractRaftRequest {

  public static InstallRequest request(long term, MemberId leader, long index, long timestamp, int offset, byte[] data, boolean complete) {
    return new InstallRequest(term, leader, index, timestamp, offset, data, complete);
  }

  private final long term;
  private final MemberId leader;
  private final long index;
  private final long timestamp;
  private final int offset;
  private final byte[] data;
  private final boolean complete;

  private InstallRequest(long term, MemberId leader, long index, long timestamp, int offset, byte[] data, boolean complete) {
    checkArgument(term > 0, "term must be positive");
    checkNotNull(leader, "leader cannot be null");
    checkArgument(index >= 0, "index must be positive");
    checkArgument(offset >= 0, "offset must be positive");
    checkNotNull(data, "data cannot be null");
    this.term = term;
    this.leader = leader;
    this.index = index;
    this.timestamp = timestamp;
    this.offset = offset;
    this.data = data;
    this.complete = complete;
  }

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the requesting leader address.
   *
   * @return The leader's address.
   */
  public MemberId leader() {
    return leader;
  }

  /**
   * Returns the snapshot index.
   *
   * @return The snapshot index.
   */
  public long snapshotIndex() {
    return index;
  }

  /**
   * Returns the snapshot timestamp.
   *
   * @return The snapshot timestamp.
   */
  public long snapshotTimestamp() {
    return timestamp;
  }

  /**
   * Returns the offset of the snapshot chunk.
   *
   * @return The offset of the snapshot chunk.
   */
  public int chunkOffset() {
    return offset;
  }

  /**
   * Returns the snapshot data.
   *
   * @return The snapshot data.
   */
  public byte[] data() {
    return data;
  }

  /**
   * Returns a boolean value indicating whether this is the last chunk of the snapshot.
   *
   * @return Indicates whether this request is the last chunk of the snapshot.
   */
  public boolean complete() {
    return complete;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, index, offset, complete, data);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof InstallRequest) {
      InstallRequest request = (InstallRequest) object;
      return request.term == term
          && request.leader == leader
          && request.index == index
          && request.offset == offset
          && request.complete == complete
          && Arrays.equals(request.data, data);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("leader", leader)
        .add("index", index)
        .add("offset", offset)
        .add("data", ArraySizeHashPrinter.of(data))
        .add("complete", complete)
        .toString();
  }
}
