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
package io.atomix.storage.journal;

/**
 * Locator.
 */
public interface JournalEntryLocator<E> {

  /**
   * Returns an integer indicating whether the desired position is before or after the current entry.
   *
   * @param entry the current entry
   * @return an integer indicating whether the desired position is before or after the current entry
   */
  int locate(Indexed<E> entry);

}
