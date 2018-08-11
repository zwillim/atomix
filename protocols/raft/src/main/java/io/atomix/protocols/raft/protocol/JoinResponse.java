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
import io.atomix.protocols.raft.cluster.RaftMember;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Server join configuration change response.
 */
public class JoinResponse extends ConfigurationResponse {

  public static JoinResponse ok(long index, long term, long timestamp, Collection<RaftMember> members) {
    return new JoinResponse(Status.OK, null, index, term, timestamp, members);
  }

  public static JoinResponse error(RaftError.Type error) {
    return new JoinResponse(Status.ERROR, new RaftError(error, null), 0, 0, 0, null);
  }

  private JoinResponse(Status status, RaftError error, long index, long term, long timestamp, Collection<RaftMember> members) {
    super(status, error, index, term, timestamp, members);
    if (status == Status.OK) {
      checkArgument(index >= 0, "index must be positive");
      checkArgument(term >= 0, "term must be positive");
      checkArgument(timestamp > 0, "time must be positive");
      checkNotNull(members, "members cannot be null");
    }
  }
}
