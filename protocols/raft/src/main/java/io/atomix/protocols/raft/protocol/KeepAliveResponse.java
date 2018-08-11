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
import io.atomix.protocols.raft.RaftError;
import io.atomix.utils.misc.ArraySizeHashPrinter;

import java.util.Collection;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Session keep alive response.
 * <p>
 * Session keep alive responses are sent upon the completion of a {@link KeepAliveRequest} from a client. Keep alive
 * responses, when successful, provide the current cluster configuration and leader to the client to ensure clients can
 * evolve with the structure of the cluster and make intelligent decisions about connecting to the cluster.
 */
public class KeepAliveResponse extends AbstractRaftResponse {

  public static KeepAliveResponse ok(MemberId leader, Collection<MemberId> members, long[] sessionIds) {
    return new KeepAliveResponse(Status.OK, null, leader, members, sessionIds);
  }

  public static KeepAliveResponse error(RaftError error) {
    return new KeepAliveResponse(Status.ERROR, error, null, null, null);
  }

  private final MemberId leader;
  private final Collection<MemberId> members;
  private final long[] sessionIds;

  public KeepAliveResponse(Status status, RaftError error, MemberId leader, Collection<MemberId> members, long[] sessionIds) {
    super(status, error);
    if (status == Status.OK) {
      checkNotNull(members, "members cannot be null");
      checkNotNull(sessionIds, "sessionIds cannot be null");
    }
    this.leader = leader;
    this.members = members;
    this.sessionIds = sessionIds;
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

  /**
   * Returns the sessions that were successfully kept alive.
   *
   * @return The sessions that were successfully kept alive.
   */
  public long[] sessionIds() {
    return sessionIds;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, leader, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof KeepAliveResponse) {
      KeepAliveResponse response = (KeepAliveResponse) object;
      return response.status == status
          && ((response.leader == null && leader == null)
          || (response.leader != null && leader != null && response.leader.equals(leader)))
          && ((response.members == null && members == null)
          || (response.members != null && members != null && response.members.equals(members)));
    }
    return false;
  }

  @Override
  public String toString() {
    if (status == Status.OK) {
      return toStringHelper(this)
          .add("status", status)
          .add("leader", leader)
          .add("members", members)
          .add("sessionIds", ArraySizeHashPrinter.of(sessionIds))
          .toString();
    } else {
      return toStringHelper(this)
          .add("status", status)
          .add("error", error)
          .toString();
    }
  }
}
