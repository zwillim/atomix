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

import io.atomix.utils.misc.ArraySizeHashPrinter;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Session keep alive request.
 * <p>
 * Keep alive requests are sent by clients to servers to maintain a session registered via a {@link OpenSessionRequest}.
 * Once a session has been registered, clients are responsible for sending keep alive requests to the cluster at a rate
 * less than the provided {@link OpenSessionResponse#timeout()}. Keep alive requests also server to acknowledge the
 * receipt of responses and events by the client. The {@link #commandSequenceNumbers()} number indicates the highest
 * command sequence number for which the client has received a response, and the {@link #eventIndexes()} numbers
 * indicate the highest index for which the client has received an event in proper sequence.
 */
public class KeepAliveRequest extends AbstractRaftRequest {

  public static KeepAliveRequest request(long[] sessionIds, long[] commandSequences, long[] eventIndexes) {
    return new KeepAliveRequest(sessionIds, commandSequences, eventIndexes);
  }

  private final long[] sessionIds;
  private final long[] commandSequences;
  private final long[] eventIndexes;

  private KeepAliveRequest(long[] sessionIds, long[] commandSequences, long[] eventIndexes) {
    this.sessionIds = checkNotNull(sessionIds, "sessionIds cannot be null");
    this.commandSequences = checkNotNull(commandSequences, "commandSequences cannot be null");
    this.eventIndexes = checkNotNull(eventIndexes, "eventIndexes cannot be null");
  }

  /**
   * Returns the session identifiers.
   *
   * @return The session identifiers.
   */
  public long[] sessionIds() {
    return sessionIds;
  }

  /**
   * Returns the command sequence numbers.
   *
   * @return The command sequence numbers.
   */
  public long[] commandSequenceNumbers() {
    return commandSequences;
  }

  /**
   * Returns the event indexes.
   *
   * @return The event indexes.
   */
  public long[] eventIndexes() {
    return eventIndexes;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), sessionIds, commandSequences, eventIndexes);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof KeepAliveRequest) {
      KeepAliveRequest request = (KeepAliveRequest) object;
      return Arrays.equals(request.sessionIds, sessionIds)
          && Arrays.equals(request.commandSequences, commandSequences)
          && Arrays.equals(request.eventIndexes, eventIndexes);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("sessionIds", ArraySizeHashPrinter.of(sessionIds))
        .add("commandSequences", ArraySizeHashPrinter.of(commandSequences))
        .add("eventIndexes", ArraySizeHashPrinter.of(eventIndexes))
        .toString();
  }
}
