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

import io.atomix.primitive.event.PrimitiveEvent;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Event publish request.
 * <p>
 * Publish requests are used by servers to publish event messages to clients. Event messages are sequenced based on the
 * point in the Raft log at which they were published to the client. The {@link #eventIndex()} indicates the index at
 * which the event was sent, and the {@link #previousIndex()} indicates the index of the prior event messages sent to
 * the client. Clients must ensure that event messages are received in sequence by tracking the last index for which
 * they received an event message and validating {@link #previousIndex()} against that index.
 */
public class PublishRequest extends SessionRequest {

  public static PublishRequest request(long session, long eventIndex, long previousIndex, List<PrimitiveEvent> events) {
    return new PublishRequest(session, eventIndex, previousIndex, events);
  }

  private final long eventIndex;
  private final long previousIndex;
  private final List<PrimitiveEvent> events;

  private PublishRequest(long session, long eventIndex, long previousIndex, List<PrimitiveEvent> events) {
    super(session);
    checkArgument(eventIndex > 0, "eventIndex must be positive");
    checkArgument(previousIndex >= 0, "previousIndex must be positive");
    checkNotNull(events, "events cannot be null");
    this.eventIndex = eventIndex;
    this.previousIndex = previousIndex;
    this.events = events;
  }

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  public long eventIndex() {
    return eventIndex;
  }

  /**
   * Returns the previous event index.
   *
   * @return The previous event index.
   */
  public long previousIndex() {
    return previousIndex;
  }

  /**
   * Returns the request events.
   *
   * @return The request events.
   */
  public List<PrimitiveEvent> events() {
    return events;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, eventIndex, previousIndex, events);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PublishRequest) {
      PublishRequest request = (PublishRequest) object;
      return request.session == session
          && request.eventIndex == eventIndex
          && request.previousIndex == previousIndex
          && request.events.equals(events);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", session)
        .add("eventIndex", eventIndex)
        .add("previousIndex", previousIndex)
        .add("events", events)
        .toString();
  }
}
