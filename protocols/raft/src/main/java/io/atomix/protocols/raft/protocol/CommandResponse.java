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

/**
 * Client command response.
 * <p>
 * Command responses are sent by servers to clients upon the completion of a {@link CommandRequest}. Command responses
 * are sent with the {@link #index()} (or index) of the state machine at the point at which the command was evaluated.
 * This can be used by the client to ensure it sees state progress monotonically. Note, however, that command responses
 * may not be sent or received in sequential order. If a command response has to await the completion of an event, or if
 * the response is proxied through another server, responses may be received out of order. Clients should resequence
 * concurrent responses to ensure they're handled in FIFO order.
 */
public class CommandResponse extends OperationResponse {

  public static CommandResponse ok(long index, long eventIndex, byte[] result, long lastSequence) {
    return new CommandResponse(Status.OK, null, index, eventIndex, result, lastSequence);
  }

  public static CommandResponse error(RaftError error) {
    return new CommandResponse(Status.ERROR, error, 0, 0, null, 0);
  }

  private CommandResponse(Status status, RaftError error, long index, long eventIndex, byte[] result, long lastSequence) {
    super(status, error, index, eventIndex, result, lastSequence);
  }
}
