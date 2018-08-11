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
 * limitations under the License.
 */
package io.atomix.protocols.raft.protocol;

import io.atomix.cluster.MemberId;

import java.util.Collection;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client heartbeat request.
 */
public class HeartbeatRequest extends AbstractRaftRequest {

  public static HeartbeatRequest request(MemberId leader, Collection<MemberId> members) {
    return new HeartbeatRequest(leader, members);
  }

  private final MemberId leader;
  private final Collection<MemberId> members;

  private HeartbeatRequest(MemberId leader, Collection<MemberId> members) {
    checkNotNull(members, "members cannot be null");
    this.leader = leader;
    this.members = members;
  }

  /**
   * Returns the cluster leader.
   *
   * @return The cluster leader.
   */
  public MemberId leader() {
    return leader;
  }

  /**
   * Returns the cluster members.
   *
   * @return The cluster members.
   */
  public Collection<MemberId> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), leader, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof HeartbeatRequest) {
      HeartbeatRequest request = (HeartbeatRequest) object;
      return Objects.equals(request.leader, leader) && Objects.equals(request.members, members);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("leader", leader)
        .add("members", members)
        .toString();
  }
}
