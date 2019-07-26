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

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.KeyValueOptions;
import io.vertx.ext.consul.SessionBehavior;
import io.vertx.ext.consul.SessionOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.vertx.core.Future.succeededFuture;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asFutureConsulEntry;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asFutureString;

/**
 * Abstract map functionality for clustering maps.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public abstract class ConsulMap<K, V> extends ConsulMapListener {

  private static final Logger log = LoggerFactory.getLogger(ConsulMap.class);

  protected ConsulMap(String name, ClusterManagerInternalContext appContext) {
    super(name, appContext);
  }

  /**
   * Puts an entry to Consul KV store.
   *
   * @param k - holds the key of an entry.
   * @param v - holds the value of an entry.
   * @return {@link Future}} containing result.
   */
  Future<Boolean> putValue(K k, V v) {
    return putValue(k, v, null);
  }

  /**
   * Puts an entry to Consul KV store by taking into account additional options
   * (these options are mainly used to make an entry ephemeral or to place TTL on an entry).
   *
   * @param k               - holds the key of an entry.
   * @param v               - holds the value of an entry.
   * @param keyValueOptions - holds kv options (note: null is allowed)
   * @return {@link Future}} containing result.
   */
  Future<Boolean> putValue(K k, V v, KeyValueOptions keyValueOptions) {
    return assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> asFutureString(k, v, appContext.getNodeId()))
      .compose(value -> putPlainValue(keyPath(k), value, keyValueOptions));
  }

  /**
   * Puts plain entry {@link String key} and {@link String value} to Consul KV store.
   *
   * @param key             - holds the consul key of an entry.
   * @param value           - holds the consul value (should be marshaled) of an entry.
   * @param keyValueOptions - holds kv options (note: null is allowed)
   * @return {@link Future}} containing result.
   */
  protected Future<Boolean> putPlainValue(String key, String value, KeyValueOptions keyValueOptions) {
    Promise<Boolean> promise = Promise.promise();
    appContext.getConsulClient().putValueWithOptions(key, value, keyValueOptions, resultHandler -> {
      if (resultHandler.succeeded()) {
        if (log.isTraceEnabled()) {
          String traceMessage = "[" + appContext.getNodeId() + "] " + key + " put is " + resultHandler.result();
          if (keyValueOptions != null) {
            log.trace(traceMessage + " with : " + keyValueOptions.getAcquireSession());
          } else {
            log.trace(traceMessage);
          }
        }
        promise.complete(resultHandler.result());
      } else {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to put " + key + " -> " + value, resultHandler.cause());
        promise.fail(resultHandler.cause());
      }
    });
    return promise.future();
  }

  /**
   * Gets the value by key.
   *
   * @param k - holds the key.
   * @return @return {@link Future}} containing result.
   */
  Future<V> getValue(K k) {
    return assertKeyIsNotNull(k)
      .compose(aVoid -> getPlainValue(keyPath(k)))
      .compose(consulValue -> asFutureConsulEntry(consulValue.getValue()))
      .compose(consulEntry -> consulEntry == null ? succeededFuture() : succeededFuture((V) consulEntry.getValue()));
  }

  /**
   * Gets the plain {@link String} value by plain {@link String} key.
   *
   * @param consulKey - holds the consul key.
   * @return @return {@link Future}} containing result.
   */
  Future<KeyValue> getPlainValue(String consulKey) {
    Promise<KeyValue> promise = Promise.promise();
    appContext.getConsulClient().getValue(consulKey, resultHandler -> {
      if (resultHandler.succeeded()) {
        // note: resultHandler.result().getValue() is null if nothing was found.
        if (log.isTraceEnabled()) {
          log.trace("[" + appContext.getNodeId() + "]" + " - Entry is found : " + resultHandler.result().getValue() + " by key: " + consulKey);
        }
        promise.complete(resultHandler.result());
      } else {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to look up an entry by: " + consulKey, resultHandler.cause());
        promise.fail(resultHandler.cause());
      }
    });
    return promise.future();
  }

  /**
   * Gets all map's entries.
   *
   * @return @return {@link Future}} containing result.
   */
  Future<Map<K, V>> entries() {
    return plainEntries()
      .compose(kvEntries -> {
        List<Future> futureList = new ArrayList<>();
        kvEntries.forEach(kv -> futureList.add(asFutureConsulEntry(kv.getValue())));
        return CompositeFuture.all(futureList).map(compositeFuture -> {
          Map<K, V> map = new HashMap<>();
          for (int i = 0; i < compositeFuture.size(); i++) {
            ConsulEntry<K, V> consulEntry = compositeFuture.resultAt(i);
            map.put(consulEntry.getKey(), consulEntry.getValue());
          }
          return map;
        });
      });
  }

  /**
   * Removes an entry by key.
   *
   * @param key holds the key.
   * @return @return {@link Future}} containing result.
   */
  Future<Boolean> deleteValue(K key) {
    return deleteValueByKeyPath(keyPath(key));
  }

  /**
   * Removes an entry by keyPath.
   *
   * @param keyPath - holds the plain {@link String} keyPath.
   * @return @return {@link Future}} containing result.
   */
  protected Future<Boolean> deleteValueByKeyPath(String keyPath) {
    Promise<Boolean> promise = Promise.promise();
    appContext.getConsulClient().deleteValue(keyPath, resultHandler -> {
      if (resultHandler.succeeded()) {
        if (log.isTraceEnabled()) {
          log.trace("[" + appContext.getNodeId() + "] " + keyPath + " -> " + " remove is true.");
        }
        promise.complete(true);
      } else {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to remove an entry by keyPath: " + keyPath, resultHandler.cause());
        promise.fail(resultHandler.cause());
      }
    });
    return promise.future();
  }

  /**
   * Deletes the entire map.
   */
  Future<Void> deleteAll() {
    Promise<Void> promise = Promise.promise();
    appContext.getConsulClient().deleteValues(name, result -> {
      if (result.succeeded()) {
        if (log.isTraceEnabled()) {
          log.trace("[" + appContext.getNodeId() + "] - has removed all of: " + name);
        }
        promise.complete();
      } else {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to clear an entire: " + name);
        promise.fail(result.cause());
      }
    });
    return promise.future();
  }

  /**
   * @return {@link Future} of plain consul kv map's keys.
   */
  protected Future<List<String>> plainKeys() {
    Promise<List<String>> futureKeys = Promise.promise();
    appContext.getConsulClient().getKeys(name, resultHandler -> {
      if (resultHandler.succeeded()) {
        if (log.isTraceEnabled()) {
          log.trace("[" + appContext.getNodeId() + "]" + " - Found following keys of: " + name + " -> " + resultHandler.result());
        }
        futureKeys.complete(resultHandler.result());
      } else {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to fetch keys of: " + name, resultHandler.cause());
        futureKeys.fail(resultHandler.cause());
      }
    });
    return futureKeys.future();
  }

  /**
   * @return {@link Future} of plain consul kv map's entries.
   */
  Future<List<KeyValue>> plainEntries() {
    Promise<List<KeyValue>> keyValueListPromise = Promise.promise();
    appContext.getConsulClient().getValues(name, resultHandler -> {
      if (resultHandler.succeeded()) keyValueListPromise.complete(nullSafeListResult(resultHandler.result()));
      else {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to fetch entries of: " + name, resultHandler.cause());
        keyValueListPromise.fail(resultHandler.cause());
      }
    });
    return keyValueListPromise.future();
  }

  /**
   * Creates consul session.
   *
   * @param sessionName - session name.
   * @param checkId     - id of the tcp check session will get bound to.
   * @return {@link Future} session id.
   */
  protected Future<String> registerSession(String sessionName, String checkId) {
    Promise<String> promise = Promise.promise();
    SessionOptions sessionOptions = new SessionOptions()
      .setBehavior(SessionBehavior.DELETE)
      .setLockDelay(0)
      .setName(sessionName)
      .setChecks(Arrays.asList(checkId, "serfHealth"));

    appContext.getConsulClient().createSessionWithOptions(sessionOptions, session -> {
      if (session.succeeded()) {
        if (log.isTraceEnabled()) {
          log.trace("[" + appContext.getNodeId() + "]" + " - " + sessionName + ": " + session.result() + " has been registered.");
        }
        promise.complete(session.result());
      } else {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to register the session.", session.cause());
        promise.fail(session.cause());
      }
    });
    return promise.future();
  }

  /**
   * Destroys node's session in consul.
   */
  protected Future<Void> destroySession(String sessionId) {
    Promise<Void> promise = Promise.promise();
    appContext.getConsulClient().destroySession(sessionId, resultHandler -> {
      if (resultHandler.succeeded()) {
        if (log.isTraceEnabled()) {
          log.trace("[" + appContext.getNodeId() + "]" + " - Session: " + sessionId + " has been successfully destroyed.");
        }
        promise.complete();
      } else {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to destroy session: " + sessionId, resultHandler.cause());
        promise.fail(resultHandler.cause());
      }
    });
    return promise.future();
  }

  /**
   * Creates TTL dedicated consul session. TTL on entries is handled by relaying on consul session itself.
   * We have to register the session first in consul and then bound the session's id with entries we want to put ttl on.
   * <p>
   * Note: session invalidation-time is twice the TTL time -> https://github.com/hashicorp/consul/issues/1172
   * (This is done on purpose. The contract of the TTL is that it will not expire before that value, but could expire after.
   * There are number of reasons for that (complexity during leadership transition), but consul devs add a grace period to account for clock skew and network delays.
   * This is to shield the application from dealing with that.)
   *
   * @param ttl - holds ttl in ms, this value must be between {@code 10s} and {@code 86400s} currently.
   * @return session id.
   * <p>
   * Note: method gets deprecated since vert.x cluster management SPI can't be satisfied with using plain consul sessions :(
   */
  @Deprecated
  protected Future<String> getTtlSessionId(long ttl, K k) {
    if (ttl < 10000) {
      log.warn("[" + appContext.getNodeId() + "]" + " - Specified ttl is less than allowed in consul -> min ttl is 10s.");
      ttl = 10000;
    }

    if (ttl > 86400000) {
      log.warn("[" + appContext.getNodeId() + "]" + " - Specified ttl is more that allowed in consul -> max ttl is 86400s.");
      ttl = 86400000;
    }

    String consulKey = keyPath(k);
    String sessionName = "ttlSession_" + consulKey;
    Future<String> future = Future.future();
    SessionOptions sessionOpts = new SessionOptions()
      .setTtl(TimeUnit.MILLISECONDS.toSeconds(ttl))
      .setBehavior(SessionBehavior.DELETE)
      // Lock delay is a time duration, between 0 and 60 seconds. When a session invalidation takes place,
      // Consul prevents any of the previously held locks from being re-acquired for the lock-delay interval
      .setLockDelay(0)
      .setName(sessionName);

    appContext.getConsulClient().createSessionWithOptions(sessionOpts, idHandler -> {
      if (idHandler.succeeded()) {
        if (log.isTraceEnabled()) {
          log.trace("[" + appContext.getNodeId() + "]" + " - TTL session has been created with id: " + idHandler.result());
        }
        future.complete(idHandler.result());
      } else {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to create ttl consul session", idHandler.cause());
        future.fail(idHandler.cause());
      }
    });
    return future;
  }


  /**
   * Obtains a result from {@link Future} by and waiting for it's completion.
   * Note: should never be called on vert.x event loop context!
   *
   * @param future  - holds the work that needs to be executed.
   * @param timeout - the maximum time to wait in ms for work to complete.
   * @param <T>     - work's result type.
   * @return actual computation result.
   */
  protected <T> T completeAndGet(Future<T> future, long timeout) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    future.setHandler(event -> {
      if (event.succeeded()) completableFuture.complete(event.result());
      else completableFuture.completeExceptionally(event.cause());
    });
    T result;
    try {
      result = completableFuture.get(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new VertxException(e);
    }
    return result;
  }

  @Override
  protected void entryUpdated(EntryEvent event) {
  }

  /**
   * Verifies whether value is not null.
   */
  Future<Void> assertValueIsNotNull(Object value) {
    boolean result = value == null;
    if (result) return io.vertx.core.Future.failedFuture("Value can not be null.");
    else return succeededFuture();
  }

  /**
   * Verifies whether key & value are not null.
   */
  Future<Void> assertKeyAndValueAreNotNull(Object key, Object value) {
    return assertKeyIsNotNull(key).compose(aVoid -> assertValueIsNotNull(value));
  }

  /**
   * Verifies whether key is not null.
   */
  Future<Void> assertKeyIsNotNull(Object key) {
    boolean result = key == null;
    if (result) return io.vertx.core.Future.failedFuture("Key can not be null.");
    else return succeededFuture();
  }

  /**
   * Builds a key path to consul map.
   * Later on this key path should be used to access any entry of given consul map.
   *
   * @param k actual key.
   * @return key path.
   */
  protected String keyPath(Object k) {
    // we can't simply ship sequence of bytes to consul.
    if (k instanceof Buffer) {
      // buffer base 64 encoded value might include double slashes which can't be accepted  by consul as a valid key.
      // see https://github.com/hashicorp/consul/issues/3476
      // for these reasons we remove all slashes out of the key ONLY if key is instance of {@link Buffer}.
      return name + "/" + (Base64.getEncoder().encodeToString(((Buffer) k).getBytes())).replaceAll("/", "");
    }
    return name + "/" + k.toString();
  }

  /**
   * Extracts an actual keyPath of consup map keyPath path.
   */
  protected String actualKey(String keyPath) {
    return keyPath.replace(name + "/", "");
  }

  /**
   * Returns NULL - safe key value list - simple wrapper around getting list out of {@link KeyValueList} instance.
   */
  List<KeyValue> nullSafeListResult(KeyValueList keyValueList) {
    return keyValueList == null || keyValueList.getList() == null ? Collections.emptyList() : keyValueList.getList();
  }

}
