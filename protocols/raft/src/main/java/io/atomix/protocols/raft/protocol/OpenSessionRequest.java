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

import io.atomix.protocols.raft.ReadConsistency;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Open session request.
 */
public class OpenSessionRequest extends AbstractRaftRequest {

  public static OpenSessionRequest request(
      String member,
      String name,
      String typeName,
      byte[] config,
      ReadConsistency readConsistency,
      long minTimeout,
      long maxTimeout) {
    return new OpenSessionRequest(member, name, typeName, config, readConsistency, minTimeout, maxTimeout);
  }

  private final String memberId;
  private final String primitiveName;
  private final String primitiveType;
  private final byte[] serviceConfig;
  private final ReadConsistency readConsistency;
  private final long minTimeout;
  private final long maxTimeout;

  private OpenSessionRequest(
      String memberId,
      String primitiveName,
      String primitiveType,
      byte[] serviceConfig,
      ReadConsistency readConsistency,
      long minTimeout,
      long maxTimeout) {
    checkArgument(minTimeout >= 0, "minTimeout must be positive");
    checkArgument(maxTimeout >= 0, "maxTimeout must be positive");
    this.memberId = checkNotNull(memberId);
    this.primitiveName = checkNotNull(primitiveName);
    this.primitiveType = checkNotNull(primitiveType);
    this.serviceConfig = checkNotNull(serviceConfig);
    this.readConsistency = checkNotNull(readConsistency);
    this.minTimeout = minTimeout;
    this.maxTimeout = maxTimeout;
  }

  /**
   * Returns the client node identifier.
   *
   * @return The client node identifier.
   */
  public String memberId() {
    return memberId;
  }

  /**
   * Returns the state machine name.
   *
   * @return The state machine name.
   */
  public String primitiveName() {
    return primitiveName;
  }

  /**
   * Returns the state machine type;
   *
   * @return The state machine type.
   */
  public String primitiveType() {
    return primitiveType;
  }

  /**
   * Returns the service configuration.
   *
   * @return the service configuration
   */
  public byte[] serviceConfig() {
    return serviceConfig;
  }

  /**
   * Returns the session read consistency level.
   *
   * @return The session's read consistency.
   */
  public ReadConsistency readConsistency() {
    return readConsistency;
  }

  /**
   * Returns the minimum session timeout.
   *
   * @return The minimum session timeout.
   */
  public long minTimeout() {
    return minTimeout;
  }

  /**
   * Returns the maximum session timeout.
   *
   * @return The maximum session timeout.
   */
  public long maxTimeout() {
    return maxTimeout;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), primitiveName, primitiveType, minTimeout, maxTimeout);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof OpenSessionRequest) {
      OpenSessionRequest request = (OpenSessionRequest) object;
      return request.memberId.equals(memberId)
          && request.primitiveName.equals(primitiveName)
          && request.primitiveType.equals(primitiveType)
          && request.readConsistency == readConsistency
          && request.minTimeout == minTimeout
          && request.maxTimeout == maxTimeout;
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("node", memberId)
        .add("serviceName", primitiveName)
        .add("serviceType", primitiveType)
        .add("readConsistency", readConsistency)
        .add("minTimeout", minTimeout)
        .add("maxTimeout", maxTimeout)
        .toString();
  }
}
