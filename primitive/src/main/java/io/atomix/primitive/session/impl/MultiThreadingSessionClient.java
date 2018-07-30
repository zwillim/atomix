/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitive.session.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primitive proxy that supports per-thread sessions.
 */
public class MultiThreadingSessionClient implements SessionClient {
  private static final SessionId DEFAULT_SESSION_ID = SessionId.from(0);
  private final PartitionId partitionId;
  private final String name;
  private final PrimitiveType primitiveType;
  private final ThreadContext context;
  private Logger log;
  private volatile PrimitiveState state = PrimitiveState.CLOSED;
  private final LoadingCache<Thread, CompletableFuture<SessionClient>> sessionCache;
  private volatile CompletableFuture<SessionClient> connectFuture;
  private volatile CompletableFuture<Void> closeFuture;

  public MultiThreadingSessionClient(
      String clientId,
      PartitionId partitionId,
      String name,
      PrimitiveType primitiveType,
      Supplier<CompletableFuture<SessionClient>> sessionFactory,
      ThreadContext context,
      int cacheSize) {
    this.partitionId = checkNotNull(partitionId);
    this.name = checkNotNull(name);
    this.primitiveType = checkNotNull(primitiveType);
    this.context = checkNotNull(context);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(SessionClient.class)
        .addValue(clientId)
        .build());
    this.sessionCache = CacheBuilder.<Thread, CompletableFuture<SessionClient>>newBuilder()
        .maximumSize(cacheSize)
        .<Thread, CompletableFuture<SessionClient>>removalListener(n -> n.getValue().thenAccept(session -> session.close()))
        .build(new CacheLoader<Thread, CompletableFuture<SessionClient>>() {
          @Override
          public CompletableFuture<SessionClient> load(Thread key) throws Exception {
            if (closeFuture != null) {
              return Futures.exceptionalFuture(new PrimitiveException.ClosedSession());
            }
            return sessionFactory.get();
          }
        });
  }

  @Override
  public SessionId sessionId() {
    Thread thread = Thread.currentThread();
    CompletableFuture<SessionClient> future = sessionCache.getIfPresent(thread);
    return future != null && future.isDone() ? future.join().sessionId() : DEFAULT_SESSION_ID;
  }

  @Override
  public PartitionId partitionId() {
    return partitionId;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return primitiveType;
  }

  @Override
  public ThreadContext context() {
    return context;
  }

  @Override
  public PrimitiveState getState() {
    return state;
  }

  @Override
  public synchronized void addStateChangeListener(Consumer<PrimitiveState> listener) {
    sessionCache.getUnchecked(Thread.currentThread())
        .thenAccept(session -> session.addStateChangeListener(listener));
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    sessionCache.getUnchecked(Thread.currentThread())
        .thenAccept(session -> session.removeStateChangeListener(listener));
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    return sessionCache.getUnchecked(Thread.currentThread())
        .thenCompose(session -> session.execute(operation));
  }

  @Override
  public synchronized void addEventListener(EventType eventType, Consumer<PrimitiveEvent> consumer) {
    sessionCache.getUnchecked(Thread.currentThread())
        .thenAccept(session -> session.addEventListener(eventType, consumer));
  }

  @Override
  public synchronized void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> consumer) {
    sessionCache.getUnchecked(Thread.currentThread())
        .thenAccept(session -> session.removeEventListener(eventType, consumer));
  }

  @Override
  public CompletableFuture<SessionClient> connect() {
    if (connectFuture == null) {
      synchronized (this) {
        if (connectFuture == null) {
          connectFuture = sessionCache.getUnchecked(Thread.currentThread());
        }
      }
    }
    return connectFuture;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = CompletableFuture.allOf(sessionCache.asMap().values().stream()
              .map(future -> future.thenCompose(session -> session.close()))
              .toArray(CompletableFuture[]::new));
        }
      }
    }
    return closeFuture;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("primitiveType", primitiveType)
        .add("state", state)
        .toString();
  }
}