/*
 * Copyright 2016-present Open Networking Foundation
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

import io.atomix.protocols.raft.cluster.RaftMember;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Member configuration change request.
 */
public class ReconfigureRequest extends ConfigurationRequest {

  public static ReconfigureRequest request(RaftMember member, long index, long term) {
    return new ReconfigureRequest(member, index, term);
  }

  private final long index;
  private final long term;

  private ReconfigureRequest(RaftMember member, long index, long term) {
    super(member);
    checkArgument(index >= 0, "index must be positive");
    checkArgument(term >= 0, "term must be positive");
    this.index = index;
    this.term = term;
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
   * Returns the configuration term.
   *
   * @return The configuration term.
   */
  public long term() {
    return term;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), index, member);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ReconfigureRequest) {
      ReconfigureRequest request = (ReconfigureRequest) object;
      return request.index == index && request.term == term && request.member.equals(member);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index)
        .add("term", term)
        .add("member", member)
        .toString();
  }
}
