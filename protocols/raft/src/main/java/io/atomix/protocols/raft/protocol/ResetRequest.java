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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Event reset request.
 * <p>
 * Reset requests are sent by clients to servers if the client receives an event message out of sequence to force the
 * server to resend events from the correct index.
 */
public class ResetRequest extends SessionRequest {

  public static ResetRequest request(long session, long index) {
    return new ResetRequest(session, index);
  }

  private final long index;

  private ResetRequest(long session, long index) {
    super(session);
    this.index = index;
  }

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  public long index() {
    return index;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, index);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ResetRequest) {
      ResetRequest request = (ResetRequest) object;
      return request.session == session
          && request.index == index;
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", session)
        .add("index", index)
        .toString();
  }
}
