/*
 * Copyright (C) 2018-2019 Roman Levytskyi
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
package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Handler;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.WatchResult;
import io.vertx.spi.cluster.consul.impl.ConsulMapListener.EntryEvent.EventType;

import java.util.*;

/**
 * Consul KV store listener encapsulates mechanism of receiving {@link io.vertx.ext.consul.KeyValueList} and transforming it to internal {@link EntryEvent}.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public abstract class ConsulMapListener {

  protected final String name;
  protected final ClusterManagerInternalContext appContext;

  ConsulMapListener(String name, ClusterManagerInternalContext appContext) {
    this.name = Objects.requireNonNull(name);
    this.appContext = Objects.requireNonNull(appContext);
  }

  /**
   * Receives an event emitted by consul watch.
   *
   * @param event - holds the event's data.
   */
  protected abstract void entryUpdated(EntryEvent event);

  protected void startListening() {
    appContext.createAndGetWatch(name).setHandler(kvWatchHandler()).start();
  }

  /**
   * Transforms incoming {@link io.vertx.ext.consul.KeyValueList} into internal {@link EntryEvent}
   * <p>
   * Note: given approach provides O(n) execution time since it must loop through prev and next lists of entries.
   */
  private Handler<WatchResult<KeyValueList>> kvWatchHandler() {
    return event -> {
      Iterator<KeyValue> nextKvIterator = getKeyValueListOrEmptyList(event.nextResult()).iterator();
      Iterator<KeyValue> prevKvIterator = getKeyValueListOrEmptyList(event.prevResult()).iterator();

      Optional<KeyValue> prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
      Optional<KeyValue> next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();

      while (prev.isPresent() || next.isPresent()) {
        // prev and next exist
        if (prev.isPresent() && next.isPresent()) {
          // keys are equal
          if (prev.get().getKey().equals(next.get().getKey())) {
            if (prev.get().getModifyIndex() == next.get().getModifyIndex()) {
              // no update since keys AND their modify indices are equal.
            } else {
              entryUpdated(new EntryEvent(EventType.WRITE, next.get()));
            }
            prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
            next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();

          } else if (prev.get().getKey().compareToIgnoreCase(next.get().getKey()) > 0) {
            entryUpdated(new EntryEvent(EventType.WRITE, next.get()));
            next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();
          } else {
            // ie -> evaluation this condition prev.get().getKey().compareToIgnoreCase(next.get().getKey()) < 0.
            entryUpdated(new EntryEvent(EventType.REMOVE, prev.get()));
            prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
          }
          continue;
        }
        if (prev.isPresent()) {
          entryUpdated(new EntryEvent(EventType.REMOVE, prev.get()));
          prev = prevKvIterator.hasNext() ? Optional.of(prevKvIterator.next()) : Optional.empty();
          continue;
        }
        entryUpdated(new EntryEvent(EventType.WRITE, next.get()));
        next = nextKvIterator.hasNext() ? Optional.of(nextKvIterator.next()) : Optional.empty();
      }
    };
  }

  /**
   * Represents an event that gets built out of consul watch's {@link io.vertx.ext.consul.KeyValueList}.
   */
  protected static final class EntryEvent {
    private final EventType eventType;
    private final KeyValue entry;

    EntryEvent(EventType eventType, KeyValue entry) {
      this.eventType = eventType;
      this.entry = entry;
    }

    public EventType getEventType() {
      return eventType;
    }

    public KeyValue getEntry() {
      return entry;
    }

    // represents an event type
    public enum EventType {
      WRITE,
      REMOVE
    }
  }

  /**
   * Simple not-null wrapper around getting key value list. As a result returns either an empty list or actual key value list.
   */
  private List<KeyValue> getKeyValueListOrEmptyList(KeyValueList keyValueList) {
    return keyValueList == null || keyValueList.getList() == null ? Collections.emptyList() : keyValueList.getList();
  }

}
