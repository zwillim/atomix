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
 * Concurrent distributed primitive builder.
 */
public abstract class ConcurrentPrimitiveBuilder<B extends ConcurrentPrimitiveBuilder<B, C, P>, C extends ConcurrentPrimitiveConfig<C>, P extends SyncPrimitive>
    extends PrimitiveBuilder<B, C, P> {
  protected ConcurrentPrimitiveBuilder(PrimitiveType type, String name, C config, PrimitiveManagementService managementService) {
    super(type, name, config, managementService);
  }

  /**
   * Sets the concurrency configuration for the primitive.
   *
   * @param concurrency the concurrency configuration
   * @return the primitive builder
   */
  public B withConcurrency(Concurrency concurrency) {
    config.setConcurrency(concurrency);
    return (B) this;
  }

  /**
   * Sets the thread pool size.
   *
   * @param threadPoolSize the thread pool size
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withThreadPoolSize(int threadPoolSize) {
    config.setThreadPoolSize(threadPoolSize);
    return (B) this;
  }
}
