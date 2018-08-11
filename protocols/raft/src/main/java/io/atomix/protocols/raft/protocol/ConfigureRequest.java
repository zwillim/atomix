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
import io.atomix.protocols.raft.cluster.RaftMember;

import java.util.Collection;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Configuration installation request.
 * <p>
 * Configuration requests are special requests that aid in installing committed configurations to passive and reserve
 * members of the cluster. Prior to the start of replication from an active member to a passive or reserve member, the
 * active member must update the passive/reserve member's configuration to ensure it is in the expected state.
 */
public class ConfigureRequest extends AbstractRaftRequest {

  public static ConfigureRequest request(long term, String leader, long index, long timestamp, Collection<RaftMember> members) {
    return new ConfigureRequest(term, leader, index, timestamp, members);
  }

  private final long term;
  private final String leader;
  private final long index;
  private final long timestamp;
  private final Collection<RaftMember> members;

  private ConfigureRequest(long term, String leader, long index, long timestamp, Collection<RaftMember> members) {
    checkArgument(term > 0, "term must be positive");
    checkNotNull(leader, "leader cannot be null");
    checkArgument(index >= 0, "index must be positive");
    checkArgument(timestamp > 0, "timestamp must be positive");
    checkNotNull(members, "members cannot be null");
    this.term = term;
    this.leader = leader;
    this.index = index;
    this.timestamp = timestamp;
    this.members = members;
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
    return MemberId.from(leader);
  }

  /**
   * Returns the configuration index.
   *
   * @return The configuration index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the configuration timestamp.
   *
   * @return The configuration timestamp.
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Returns the configuration members.
   *
   * @return The configuration members.
   */
  public Collection<RaftMember> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, index, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ConfigureRequest) {
      ConfigureRequest request = (ConfigureRequest) object;
      return request.term == term
          && request.leader == leader
          && request.index == index
          && request.timestamp == timestamp
          && request.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("leader", leader)
        .add("index", index)
        .add("timestamp", timestamp)
        .add("members", members)
        .toString();
  }
}
