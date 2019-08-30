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

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asConsulEntry;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asFutureConsulEntry;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asFutureString;

/**
 * Distributed async multimap implementation backed by consul kv store. IMPORTANT: the purpose of async multimap in vertx cluster management is to hold mapping between
 * event bus names (addresses) and their actual subscribers (subscriber is simply an entry containing host and port). When a message is fired from producer through
 * event bus to particular address (which is simple string), first - address gets resolved by cluster manager by looking up entry (entries) where key is event bus address and value
 * is one or set of corresponding IP addresses -> where a message is going to be routed to.
 * <p>
 * <b>Implementation details:</b>
 * <p>
 * - Consul itself doesn't provide the multimap implementation out-of-the box - this is (to be) addressed locally.
 * Entries of vertx event-bus subscribers MUST BE EPHEMERAL (AsyncMultiMap holds the subscribers) so node id is appended to each key of this map.
 * Example :
 * __vertx.subs/{address1}/{nodeId} -> Set<V>
 * __vertx.subs/{address1}/{nodeId} -> Set<V>
 * __vertx.subs/{address2}/{nodeId} -> Set<V>
 * __vertx.subs/{address3}/{nodeId} -> Set<V>
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulAsyncMultiMap<K, V> extends ConsulMap<K, V> implements AsyncMultiMap<K, V> {

  private final static Logger log = LoggerFactory.getLogger(ConsulAsyncMultiMap.class);

  private final TaskQueue taskQueue = new TaskQueue();
  private final KeyValueOptions kvOpts;


  public ConsulAsyncMultiMap(String name, ClusterManagerInternalContext appContext) {
    super(name, appContext);
    // options to make entries of this map ephemeral.
    this.kvOpts = new KeyValueOptions().setAcquireSession(appContext.getEphemeralSessionId());
  }

  private static String getRidOfNodeId(String consulKeyPath) {
    return consulKeyPath.substring(0, consulKeyPath.lastIndexOf("/"));
  }

  @Override
  public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> getAllByKey(keyPathForAllByAddressAndByNodeId(k, appContext.getNodeId())))
      .compose(vs -> doAdd(k, v, vs))
      .setHandler(completionHandler);
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> getAll(keyPathForAllByAddress(k)))
      .compose(consulEntries -> {
        List<Future> futures = new ArrayList<>();
        consulEntries.forEach(consulEntry -> futures.add(doDelete(consulEntry.getKey(), v, toChoosableSet(consulEntry.getValue()), consulEntry.getNodeId())));
        return CompositeFuture.all(futures).map(compositeFuture -> {
          for (int i = 0; i < compositeFuture.size(); i++) {
            boolean resAt = compositeFuture.resultAt(i);
            if (!resAt) return false;
          }
          return true;
        });
      })
      .setHandler(completionHandler);
  }

  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
    removeAllMatching(v::equals, completionHandler);
  }

  @Override
  public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
    getAll(keyPathForAll())
      .compose(consulEntries -> {
        List<Future> futures = new ArrayList<>();
        consulEntries.forEach(consulEntry ->
          consulEntry.getValue().forEach(v -> {
            if (p.test(v)) {
              futures.add(doDelete(consulEntry.getKey(), v, toChoosableSet(consulEntry.getValue()), consulEntry.getNodeId()));
            }
          }));
        return CompositeFuture.all(futures).compose(compositeFuture -> Future.<Void>succeededFuture());
      }).setHandler(completionHandler);
  }

  @Override
  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> resultHandler) {
    assertKeyIsNotNull(k)
      .compose(aVoid -> doGet(k))
      .compose(vs -> succeededFuture((ChoosableIterable<V>) vs))
      .setHandler(resultHandler);
  }

  /**
   * Puts an entry to consul kv store.
   *
   * @param k       represents key of the entry (i.e. event bus address).
   * @param v       represents value of the entry (i.e. location of event bus subscribers).
   * @param entries holds entries to which the new entries will be added, these entries have to be queried first.
   * @return {@link Future}
   */
  Future<Void> doAdd(K k, V v, Set<V> entries) {
    Set<V> newOne = new HashSet<>(entries);
    newOne.add(v);
    return addToConsulKv(k, newOne, appContext.getNodeId())
      .compose(aBoolean -> aBoolean ? succeededFuture() : failedFuture(v.toString() + ": wasn't added to: " + name));
  }

  /**
   * Gets an entry from consul kv store
   * @param key represents key of the entry (i.e. event bus address).
   * @return set of subscribers.
   */

  Future<ChoosableSet<V>> doGet(K key) {
    /*
     * We are wrapping async call into sync and execute it on the taskQueue. This way we maintain the order
     * in which "get" tasks are executed.
     * If we simply implement this method as : return preferConsistency ? nonCacheableGet(key) : cacheableGet(key);
     * then {@link ClusteredEventBusTest.sendNoContext} will fail due to the fact async calls to get subs by key are unordered.
     * TODO: Is there any way in vert.x ecosystem to execute tasks on the event loop by not giving up an order ?
     */
    Promise<ChoosableSet<V>> out = Promise.promise();
    VertxInternal vertxInternal = (VertxInternal) appContext.getVertx();
    vertxInternal.getOrCreateContext().<ChoosableSet<V>>executeBlocking(event -> {
      Future<ChoosableSet<V>> future = getAllByKey(keyPathForAllByAddress(key)).compose(vs -> succeededFuture(toChoosableSet(vs)));
      ChoosableSet<V> choosableSet = completeAndGet(future, 5000);
      event.complete(choosableSet);
    }, taskQueue, res -> out.complete(res.result()));
    return out.future();
  }

  /**
   * Deletes then entry from consul kv store.
   *
   * @param key    represents key of the entry (i.e. event bus address).
   * @param value  represents value of the entry (i.e. location of event bus subscriber).
   * @param from   holds {@link ChoosableSet} of entries from which the entry will get an attempt to be removed, those have to be queried first.
   * @param nodeId represents node id that the entry belongs to.
   * @return {@link Future}
   */
  Future<Boolean> doDelete(K key, V value, ChoosableSet<V> from, String nodeId) {
    if (from.remove(value)) {
      if (from.isEmpty()) return deleteValueByKeyPath(keyPathForAllByAddressAndByNodeId(key, nodeId));
      else return addToConsulKv(key, toHashSet(from), nodeId);
    } else {
      return Future.succeededFuture(false);
    }
  }

  private Future<Boolean> addToConsulKv(K key, Set<V> vs, String nodeId) {
    return asFutureString(key, vs, nodeId)
      .compose(encodedValue -> putPlainValue(keyPathForAllByAddressAndByNodeId(key, nodeId), encodedValue, kvOpts));
  }

  /**
   * Returns a consul key path used to fetch all entries (all subscribers of all event buses that are registered).
   */
  private String keyPathForAll() {
    return name;
  }

  /**
   * Returns a consul key path used to fetch all entries filtered by key (all subscribers of specified event bus address).
   */
  private String keyPathForAllByAddress(K key) {
    return keyPathForAll() + "/" + key.toString();
  }

  /**
   * Returns a consul key path used to fetch all entries filtered by key and by node id (all subscribers of specified event bus address
   * that belongs to specified node).
   */
  private String keyPathForAllByAddressAndByNodeId(K key, String nodeId) {
    return keyPathForAllByAddress(key) + "/" + nodeId;
  }

  /**
   * Returns {@link Set} of all entries filtered by specified consul key path.
   */
  private Future<Set<V>> getAllByKey(String consulKeyPath) {
    return getAll(consulKeyPath)
      .compose(entries -> succeededFuture(entries
        .stream()
        .map(ConsulEntry::getValue)
        .flatMap(Set::stream)
        .collect(Collectors.toSet())));
  }

  /**
   * Returns an set of an internal {@link ConsulEntry} all entries filtered by specified consul key path.
   */
  private Future<Set<ConsulEntry<K, Set<V>>>> getAll(String consulKey) {
    Promise<KeyValueList> promise = Promise.promise();
    appContext.getConsulClient().getValues(consulKey, promise);

    return promise.future().compose(keyValueList -> {
      List<KeyValue> keyValues = nullSafeListResult(keyValueList);
      List<Future> futures = new ArrayList<>();
      keyValues
        .stream()
        .filter(
          keyValue ->
            (consulKey.equals(name)) || keyValue.getKey().equals(consulKey) || getRidOfNodeId(keyValue.getKey()).equals(consulKey)
        )
        .forEach(keyValue -> futures.add(asFutureConsulEntry(keyValue.getValue())));

      return CompositeFuture.all(futures).map(compositeFuture -> {
        Set<ConsulEntry<K, Set<V>>> resultSet = new HashSet<>();
        for (int i = 0; i < compositeFuture.size(); i++) {
          resultSet.add(compositeFuture.resultAt(i));
        }
        return resultSet;
      });
    });
  }

  private ChoosableSet<V> toChoosableSet(Set<V> set) {
    ChoosableSet<V> choosableSet = new ChoosableSet<>(set.size());
    set.forEach(choosableSet::add);
    return choosableSet;
  }

  private Set<V> toHashSet(ChoosableSet<V> set) {
    Set<V> hashSet = new HashSet<>(set.size());
    set.forEach(hashSet::add);
    return hashSet;
  }
}
