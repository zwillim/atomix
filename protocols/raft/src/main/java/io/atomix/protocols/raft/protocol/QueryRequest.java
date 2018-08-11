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

import io.atomix.primitive.operation.PrimitiveOperation;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Client query request.
 * <p>
 * Query requests are submitted by clients to the Raft cluster to commit {@link PrimitiveOperation}s to the replicated
 * state machine. Each query request must be associated with a registered {@link #session()} and have a unique {@link
 * #sequenceNumber()} number within that session. Queries will be applied in the cluster in the order defined by the
 * provided sequence number. Thus, sequence numbers should never be skipped. In the event of a failure of a query
 * request, the request should be resent with the same sequence number. Queries are guaranteed to be applied in sequence
 * order.
 */
public class QueryRequest extends OperationRequest {

  public static QueryRequest request(long session, long sequence, PrimitiveOperation operation, long index) {
    return new QueryRequest(session, sequence, operation, index);
  }

  private final long index;

  private QueryRequest(long session, long sequence, PrimitiveOperation operation, long index) {
    super(session, sequence, operation);
    checkArgument(index >= 0, "index must be positive");
    this.index = index;
  }

  /**
   * Returns the query index.
   *
   * @return The query index.
   */
  public long index() {
    return index;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, sequence, operation, index);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof QueryRequest) {
      QueryRequest request = (QueryRequest) object;
      return request.session == session
          && request.sequence == sequence
          && request.operation.equals(operation);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", session)
        .add("sequence", sequence)
        .add("operation", operation)
        .add("index", index)
        .toString();
  }
}
