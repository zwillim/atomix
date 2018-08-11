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

import io.atomix.protocols.raft.RaftError;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Server append entries response.
 */
public class AppendResponse extends AbstractRaftResponse {

  public static AppendResponse ok(long term, boolean succeeded, long lastLogIndex) {
    return new AppendResponse(Status.OK, null, term, succeeded, lastLogIndex);
  }

  public static AppendResponse error(RaftError error) {
    return new AppendResponse(Status.ERROR, error, 0, false, 0);
  }

  private final long term;
  private final boolean succeeded;
  private final long lastLogIndex;

  private AppendResponse(Status status, RaftError error, long term, boolean succeeded, long lastLogIndex) {
    super(status, error);
    if (status == Status.OK) {
      checkArgument(term > 0, "term must be positive");
      checkArgument(lastLogIndex >= 0, "lastLogIndex must be positive");
    }
    this.term = term;
    this.succeeded = succeeded;
    this.lastLogIndex = lastLogIndex;
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
   * Returns a boolean indicating whether the append was successful.
   *
   * @return Indicates whether the append was successful.
   */
  public boolean succeeded() {
    return succeeded;
  }

  /**
   * Returns the last index of the replica's log.
   *
   * @return The last index of the responding replica's log.
   */
  public long lastLogIndex() {
    return lastLogIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, term, succeeded, lastLogIndex);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof AppendResponse) {
      AppendResponse response = (AppendResponse) object;
      return response.status == status
          && response.term == term
          && response.succeeded == succeeded
          && response.lastLogIndex == lastLogIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    if (status == Status.OK) {
      return toStringHelper(this)
          .add("status", status)
          .add("term", term)
          .add("succeeded", succeeded)
          .add("lastLogIndex", lastLogIndex)
          .toString();
    } else {
      return toStringHelper(this)
          .add("status", status)
          .add("error", error)
          .toString();
    }
  }
}
