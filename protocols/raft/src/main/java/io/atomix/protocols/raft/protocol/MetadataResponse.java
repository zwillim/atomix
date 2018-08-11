/*
 * Copyright 2017-present Open Networking Foundation
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

import io.atomix.primitive.session.SessionMetadata;
import io.atomix.protocols.raft.RaftError;

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster metadata response.
 */
public class MetadataResponse extends AbstractRaftResponse {

  public static MetadataResponse ok(Set<SessionMetadata> sessions) {
    return new MetadataResponse(Status.OK, null, sessions);
  }

  public static MetadataResponse error(RaftError error) {
    return new MetadataResponse(Status.ERROR, error, null);
  }

  private final Set<SessionMetadata> sessions;

  private MetadataResponse(Status status, RaftError error, Set<SessionMetadata> sessions) {
    super(status, error);
    if (status == Status.OK) {
      checkNotNull(sessions, "sessions cannot be null");
    }
    this.sessions = sessions;
  }

  /**
   * Returns the session metadata.
   *
   * @return Session metadata.
   */
  public Set<SessionMetadata> sessions() {
    return sessions;
  }

  @Override
  public String toString() {
    if (status == Status.OK) {
      return toStringHelper(this)
          .add("status", status)
          .add("sessions", sessions)
          .toString();
    } else {
      return toStringHelper(this)
          .add("status", status)
          .add("error", error)
          .toString();
    }
  }
}
