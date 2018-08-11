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

import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.cluster.RaftMember;

import java.util.Collection;

/**
 * Server configuration change response.
 */
public class ReconfigureResponse extends ConfigurationResponse {

  public static ReconfigureResponse ok(long index, long term, long timestamp, Collection<RaftMember> members) {
    return new ReconfigureResponse(Status.OK, null, index, term, timestamp, members);
  }

  public static ReconfigureResponse error(RaftError.Type error) {
    return new ReconfigureResponse(Status.ERROR, error != null ? new RaftError(error, null) : null, 0, 0, 0, null);
  }

  private ReconfigureResponse(Status status, RaftError error, long index, long term, long timestamp, Collection<RaftMember> members) {
    super(status, error, index, term, timestamp, members);
  }
}
