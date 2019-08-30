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

import io.vertx.core.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.*;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asFutureString;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asTtlConsulEntry;

/**
 * Distributed async map implementation that is backed by consul key-value store.
 * <p>
 * Note: given map is used by vertx nodes to share the data,
 * entries of this map are always PERSISTENT and NOT EPHEMERAL.
 * <p>
 * For ttl handling see {@link TTLMonitor}
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulAsyncMap<K, V> extends ConsulMap<K, V> implements AsyncMap<K, V> {

  private static final Logger log = LoggerFactory.getLogger(ConsulAsyncMap.class);
  private final TTLMonitor ttlMonitor;

  public ConsulAsyncMap(String name, ClusterManagerInternalContext appContext, ClusterManager clusterManager) {
    super(name, appContext);
    this.ttlMonitor = new TTLMonitor(appContext.getVertx(), clusterManager, name, appContext.getNodeId());
    startListening();
  }

  @Override
  public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    assertKeyIsNotNull(k)
      .compose(aVoid -> getValue(k))
      .setHandler(asyncResultHandler);
  }

  @Override
  public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> putValue(k, v, null, Optional.empty()))
      .compose(putSucceeded -> putSucceeded ? Future.<Void>succeededFuture() : failedFuture(k.toString() + "wasn't put to: " + name))
      .setHandler(completionHandler);
  }

  @Override
  public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(id -> putValue(k, v, null, Optional.of(ttl)))
      .compose(putSucceeded -> putSucceeded ? succeededFuture() : Future.<Void>failedFuture(k.toString() + "wasn't put to " + name))
      .setHandler(completionHandler);
  }

  @Override
  public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
    putIfAbsent(k, v, Optional.empty()).setHandler(completionHandler);
  }

  @Override
  public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> putIfAbsent(k, v, Optional.of(ttl)))
      .setHandler(completionHandler);
  }

  @Override
  public void remove(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    assertKeyIsNotNull(k).compose(aVoid -> {
      Promise<V> promise = Promise.promise();
      get(k, promise);
      return promise.future();
    }).compose(v -> {
      Promise<V> promise = Promise.promise();
      if (v == null) promise.complete();
      else deleteValueByKeyPath(keyPath(k))
        .compose(removeSucceeded -> removeSucceeded ? succeededFuture(v) : failedFuture("Key + " + k + " wasn't removed."))
        .setHandler(promise);
      return promise.future();
    }).setHandler(asyncResultHandler);
  }

  @Override
  public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
    // removes a value from the map, only if entry already exists with same value.
    assertKeyAndValueAreNotNull(k, v).compose(aVoid -> {
      Promise<V> promise = Promise.promise();
      get(k, promise);
      return promise.future();
    }).compose(value -> {
      if (v.equals(value))
        return deleteValueByKeyPath(keyPath(k))
          .compose(removeSucceeded -> removeSucceeded ? succeededFuture(true) : failedFuture("Key + " + k + " wasn't removed."));
      else return succeededFuture(false);
    }).setHandler(resultHandler);
  }

  @Override
  public void replace(K k, V v, Handler<AsyncResult<V>> asyncResultHandler) {
    // replaces the entry only if it is currently mapped to some value.
    assertKeyAndValueAreNotNull(k, v).compose(aVoid -> {
      Promise<V> promise = Promise.promise();
      get(k, promise);
      return promise.future();
    }).compose(value -> {
      Promise<V> promise = Promise.promise();
      if (value == null) {
        promise.complete();
      } else {
        put(k, v, event -> {
          if (event.succeeded()) promise.complete(value);
          else promise.fail(event.cause());
        });
      }
      return promise.future();
    }).setHandler(asyncResultHandler);
  }

  @Override
  public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
    // replaces the entry only if it is currently mapped to a specific value.
    assertKeyAndValueAreNotNull(k, oldValue)
      .compose(aVoid -> assertValueIsNotNull(newValue))
      .compose(aVoid -> {
        Promise<V> promise = Promise.promise();
        get(k, promise);
        return promise.future();
      })
      .compose(value -> {
        Promise<Boolean> promise = Promise.promise();
        if (value != null) {
          if (value.equals(oldValue))
            put(k, newValue, resultPutHandler -> {
              if (resultPutHandler.succeeded())
                promise.complete(true); // old V: '{}' has been replaced by new V: '{}' where K: '{}'", oldValue, newValue, k
              else
                promise.fail(resultPutHandler.cause()); // failed replace old V: '{}' by new V: '{}' where K: '{}' due to: '{}'", oldValue, newValue, k, resultPutHandler.cause()
            });
          else
            promise.complete(false); // "An entry with K: '{}' doesn't map to old V: '{}' so it won't get replaced.", k, oldValue);
        } else promise.complete(false); // An entry with K: '{}' doesn't exist,
        return promise.future();
      })
      .setHandler(resultHandler);
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> resultHandler) {
    deleteAll().setHandler(resultHandler);
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> resultHandler) {
    plainKeys().compose(list -> succeededFuture(list.size())).setHandler(resultHandler);
  }

  @Override
  public void keys(Handler<AsyncResult<Set<K>>> asyncResultHandler) {
    entries().compose(kvMap -> succeededFuture(kvMap.keySet())).setHandler(asyncResultHandler);
  }

  @Override
  public void values(Handler<AsyncResult<List<V>>> asyncResultHandler) {
    entries().compose(kvMap -> Future.<List<V>>succeededFuture(new ArrayList<>(kvMap.values())).setHandler(asyncResultHandler));
  }

  @Override
  public void entries(Handler<AsyncResult<Map<K, V>>> asyncResultHandler) {
    entries().setHandler(asyncResultHandler);
  }

  @Override
  protected void entryUpdated(EntryEvent event) {
    if (event.getEventType() == EntryEvent.EventType.WRITE) {
      if (log.isDebugEnabled()) {
        log.debug("[" + appContext.getNodeId() + "] : " + "applying a ttl monitor on entry: " + event.getEntry().getKey());
      }
      ttlMonitor.apply(
        event.getEntry().getKey(),
        asTtlConsulEntry(event.getEntry().getValue()));
    }
  }

  /**
   * Puts an entry only if there is no entry with the key already present. If key already present then the existing
   * value will be returned to the handler, otherwise null.
   *
   * @param k - holds the entry's key.
   * @param v - holds the entry's value.
   * @return future existing value if k is already present, otherwise future null.
   */
  private Future<V> putIfAbsent(K k, V v, Optional<Long> ttl) {
    // set the Check-And-Set index. If the index is {@code 0}, Consul will only put the key if it does not already exist.
    KeyValueOptions casOpts = new KeyValueOptions().setCasIndex(0);
    return putValue(k, v, casOpts, ttl).compose(putSucceeded -> {
      if (putSucceeded) return succeededFuture();
      else return getValue(k); // key already present
    });
  }

  /**
   * Puts an entry by taking into account TTL.
   *
   * @param k               - holds the entry's key.
   * @param v               - holds the entry's value.
   * @param keyValueOptions - cas, this might be null.
   * @param ttl             - ttl on entry in ms.
   * @return {@link Future} holding the result.
   */
  private Future<Boolean> putValue(K k, V v, KeyValueOptions keyValueOptions, Optional<Long> ttl) {
    Long ttlValue = ttl.map(aLong -> ttl.get()).orElse(null);
    return asFutureString(k, v, appContext.getNodeId(), ttlValue)
      .compose(value -> putPlainValue(keyPath(k), value, keyValueOptions))
      .compose(result ->
        ttlMonitor.apply(keyPath(k), ttl)
          .compose(aVoid -> succeededFuture(result)));
  }

  /**
   * Dedicated to {@link AsyncMap} TTL monitor to handle the ability to place ttl on map's entries.
   * <p>
   * IMPORTANT:
   * TTL can placed on consul entries by relaying on consul sessions (first we have to register ttl session and bind it with an entry) - once
   * session gets expired (session's ttl gets expired) -> all entries given session was bound to get removed automatically from consul kv store.
   * Consul's got following restriction :
   * - Session TTL value must be between 10s and 86400s -> we can't really rely on using consul sessions since it breaks general
   * vert.x cluster management SPI. This also should be taken into account:
   * [Invalidation-time is twice the TTL time](https://github.com/hashicorp/consul/issues/1172) -> actual time when ttl entry gets removed (expired)
   * is doubled to what you will specify as a ttl.
   * <p>
   * For these reasons custom TTL monitoring mechanism was developed.
   *
   * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
   */
  private static class TTLMonitor {
    private final static Logger log = LoggerFactory.getLogger(TTLMonitor.class);
    private final Vertx vertx;
    private final ClusterManager clusterManager;
    /*
     * TTL monitor needs to hold a state - i.e. some sort of correlation between scheduled timers and
     * corresponding keypaths on which ttl action is gonna get executed. We use vert.x {@link LocalMap}
     * to hold this state. Having this allows us to either stop OR reschedule an appropriate timer.
     */
    private final LocalMap<String, Long> timerMap;
    private final String nodeId;
    private final String mapName;


    TTLMonitor(Vertx vertx, ClusterManager clusterManager, String mapName, String nodeId) {
      this.vertx = vertx;
      this.timerMap = vertx.sharedData().getLocalMap("timerMap");
      this.clusterManager = clusterManager;
      this.mapName = mapName;
      this.nodeId = nodeId;
    }

    /**
     * Monitors specified keypath on TTL.
     * If ttl is not present then we attempt to cancel or reschedule the timer (if it was already scheduled on specified keypath.)
     * If ttl is present we attempt to get a lock and schedule an appropriate vert.x scheduler.
     *
     * @param keyPath represents consul key path that will be monitored.
     * @param ttl     time to live in ms.
     * @return
     */
    Future<Void> apply(String keyPath, Optional<Long> ttl) {
      Promise<Void> promise = Promise.promise();
      if (ttl.isPresent()) {
        if (log.isDebugEnabled()) {
          log.debug("[" + nodeId + "] : " + "applying ttl monitoring on: " + keyPath + " with ttl: " + ttl.get());
        }
        String lockName = "ttlLockOn/" + keyPath;
        clusterManager.getLockWithTimeout(lockName, 50, lockObtainedEvent -> {
          setTimer(keyPath, ttl.get(), lockName, lockObtainedEvent);
          promise.complete();
        });
      } else {
        // there's no need to monitor an entry -> no ttl is there.
        // try to remove keyPath from timerMap
        cancelTimer(keyPath);
        promise.complete();
      }
      return promise.future();
    }

    /**
     * Schedules a timer for ttl action to take place.
     *
     * @param keyPath   consul key path.
     * @param ttl       the delay in milliseconds, after which the timer will fire.
     * @param lockName  lock name that is being put to execute the ttl action.
     * @param lockEvent lock event holding an actual lock.
     */
    private void setTimer(String keyPath, long ttl, String lockName, AsyncResult<Lock> lockEvent) {
      long timerId = vertx.setTimer(ttl, event -> {
        if (lockEvent.succeeded()) {
          deleteTTLEntry(keyPath, lockEvent.result());
        } else {
          clusterManager.getLockWithTimeout(lockName, 1000, lockObtainedEvent -> {
            if (lockObtainedEvent.succeeded()) {
              deleteTTLEntry(keyPath, lockObtainedEvent.result());
            }
          });
        }
      });
      timerMap.put(keyPath, timerId);
    }

    /**
     * Cancels vert.x timer and updates timer map accordingly.
     */
    private void cancelTimer(String keyPath) {
      Long timerId = timerMap.get(keyPath);
      if (Objects.nonNull(timerId)) {
        vertx.cancelTimer(timerId);
        timerMap.remove(keyPath);
        if (log.isDebugEnabled()) {
          log.debug("[" + nodeId + "] : " + "cancelling ttl monitoring on entry: " + keyPath);
        }
      }
    }

    /**
     * Deletes an entry that ttl was bound to.
     * We lock delete operation and once delete is executed:
     * 1) we release the lock.
     * 2) we update timer map by removing timer id that has triggered delete operation.
     *
     * @param keyPath entry's keypath in consul kv store.
     * @param lock    lock holding the delete operation.
     */
    private void deleteTTLEntry(String keyPath, Lock lock) {
      clusterManager.getAsyncMap(mapName, asyncMapEvent -> {
        ConsulAsyncMap asyncMap = (ConsulAsyncMap) asyncMapEvent.result();
        asyncMap.deleteValueByKeyPath(keyPath).setHandler(deleteResult -> {
          lock.release();
          timerMap.remove(keyPath);
        });
      });
    }
  }
}
