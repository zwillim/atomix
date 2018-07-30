/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive;

/**
 * Concurrent primitive configuration.
 */
public abstract class ConcurrentPrimitiveConfig<C extends CachedPrimitiveConfig<C>> extends CachedPrimitiveConfig<C> {
  private Concurrency concurrency = Concurrency.SERIAL;
  private int threadPoolSize = Runtime.getRuntime().availableProcessors();

  /**
   * Returns the primitive concurrency strategy.
   *
   * @return the concurrency strategy
   */
  public Concurrency getConcurrency() {
    return concurrency;
  }

  /**
   * Sets the primitive concurrency strategy.
   *
   * @param concurrency the concurrency strategy
   * @return the primitive configuration
   */
  @SuppressWarnings("unchecked")
  public C setConcurrency(Concurrency concurrency) {
    this.concurrency = concurrency;
    return (C) this;
  }

  /**
   * Returns the thread pool size.
   *
   * @return the thread pool size
   */
  public int getThreadPoolSize() {
    return threadPoolSize;
  }

  /**
   * Sets the thread pool size.
   *
   * @param threadPoolSize the thread pool size
   * @return the primitive configuration
   */
  @SuppressWarnings("unchecked")
  public C setThreadPoolSize(int threadPoolSize) {
    this.threadPoolSize = threadPoolSize;
    return (C) this;
  }
}
