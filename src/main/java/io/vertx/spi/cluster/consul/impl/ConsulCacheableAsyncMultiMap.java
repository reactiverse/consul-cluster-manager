/*
 * Copyright (C) 2019 Roman Levytskyi
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

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.vertx.core.Future.succeededFuture;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asConsulEntry;

/**
 * {@link ConsulAsyncMultiMap} implementation with caching capabilities.
 * <p>
 * Concurrent hash map is used to implement the local IN-MEMORY multimap cache.
 * Cache is enabled ONLY when {@code preferConsistency} is set to false i.e. availability (better latency) is preferred.
 * If cache is enabled:
 * Cache read operations happen synchronously by simply reading from {@link java.util.concurrent.ConcurrentHashMap}.
 * Cache WRITE operations happen either:
 * - through consul watch that monitors the consul kv store for updates (see https://www.consul.io/docs/agent/watches.html).
 * - when consul agent acknowledges the success of write operation (local node's data gets immediately cached without even waiting for a watch to take place.)
 * Note: local cache updates still might kick in through consul watch in case update succeeded in consul agent but wasn't yet acknowledged back to node. Eventually last write wins.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulCacheableAsyncMultiMap<K, V> extends ConsulAsyncMultiMap<K, V> {

  private final static Logger log = LoggerFactory.getLogger(ConsulCacheableAsyncMultiMap.class);

  private ConcurrentMap<K, ChoosableSet<V>> cache;

  public ConsulCacheableAsyncMultiMap(String name, ClusterManagerInternalContext appContext) {
    super(name, appContext);
    cache = new ConcurrentHashMap<>();
    startListening();
  }

  @Override
  Future<Void> doAdd(K k, V v, Set<V> entries) {
    return super.doAdd(k, v, entries).compose(aVoid -> {
      addEntryToCache(k, v);
      return succeededFuture();
    });
  }

  @Override
  Future<ChoosableSet<V>> doGet(K key) {
    ChoosableSet<V> cachedEntries = cache.get(key);
    if (Objects.nonNull(cachedEntries)) {
      return succeededFuture(cachedEntries);
    } else {
      return super.doGet(key).compose(vs -> {
        addEntriesToCache(key, vs);
        return succeededFuture(vs);
      });
    }
  }

  @Override
  Future<Boolean> doDelete(K key, V value, ChoosableSet<V> from, String nodeId) {
    return super.doDelete(key, value, from, nodeId).compose(isDeleted -> {
      if (isDeleted) {
        removeEntryFromCache(key, value);
      }
      return succeededFuture(isDeleted);
    });
  }

  private void addEntryToCache(K key, V value) {
    ChoosableSet<V> choosableSet = cache.get(key);
    if (choosableSet == null) choosableSet = new ChoosableSet<>(1);
    choosableSet.add(value);
    cache.put(key, choosableSet);
    if (log.isTraceEnabled()) {
      log.trace("[" + appContext.getNodeId() + "]" + " Cache: " + name + " after put of " + key + " -> " + value + ": " + Json.encode(cache));
    }
  }

  private void removeEntryFromCache(K key, V value) {
    ChoosableSet<V> choosableSet = cache.get(key);
    if (choosableSet == null) return;
    choosableSet.remove(value);
    if (choosableSet.isEmpty()) cache.remove(key);
    else cache.put(key, choosableSet);
    if (log.isTraceEnabled()) {
      log.trace("[" + appContext.getNodeId() + "]" + " Cache: " + name + " after remove of " + key + " -> " + value + ": " + Json.encode(cache));
    }
  }

  private void addEntriesToCache(K key, ChoosableSet<V> values) {
    cache.put(key, values);
  }

  @Override
  protected synchronized void entryUpdated(EntryEvent event) {
    if (log.isTraceEnabled()) {
      log.trace("[" + appContext.getNodeId() + "]" + " Entry: " + event.getEntry().getKey() + " is for " + event.getEventType());
    }
    ConsulEntry<K, Set<V>> entry;
    try {
      entry = asConsulEntry(event.getEntry().getValue());
    } catch (Exception e) {
      log.warn("Failed to decode: " + event.getEntry().getKey() + " -> " + event.getEntry().getValue(), e);
      return;
    }
    switch (event.getEventType()) {
      case WRITE:
        entry.getValue().forEach(v -> addEntryToCache(entry.getKey(), v));
        break;
      case REMOVE:
        entry.getValue().forEach(v -> removeEntryFromCache(entry.getKey(), v));
        break;
      default:
        break;
    }
  }
}
