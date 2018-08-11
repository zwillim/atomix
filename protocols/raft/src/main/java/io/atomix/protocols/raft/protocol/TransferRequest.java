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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Leadership transfer request.
 */
public class TransferRequest extends AbstractRaftRequest {

  public static TransferRequest request(MemberId member) {
    return new TransferRequest(member);
  }

  protected final MemberId member;

  private TransferRequest(MemberId member) {
    checkNotNull(member, "member cannot be null");
    this.member = member;
  }

  /**
   * Returns the member to which to transfer.
   *
   * @return The member to which to transfer.
   */
  public MemberId member() {
    return member;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), member);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) return true;
    if (object == null || !getClass().isAssignableFrom(object.getClass())) return false;

    return ((TransferRequest) object).member.equals(member);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("member", member)
        .toString();
  }
}
