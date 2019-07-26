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
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.shareddata.Counter;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.Objects;

/**
 * Consul-based implementation of an asynchronous (distributed) counter that can be used across the cluster to maintain a consistent count.
 * <p>
 * <b> Given implementation is based on Check-and-Set consul operation.</b>
 * Some notes:
 * <p>
 * The purpose of the Check-and-Set operation is to avoid lost updates when multiple clients are simultaneously trying to update a value
 * of the same key. Check-and-Set operation allows the update to happen only if the value has not been changed since the client last read it.
 * If the current value does not match what the client previously read, the client will receive a conflicting update error message and
 * will have to retry the read-update cycle. The Check-and-Set operation can be used to implement a shared counter, semaphore or a distributed lock
 * - and this is what we need.
 * <p>
 * Good to read: http://alesnosek.com/blog/2017/07/25/check-and-set-operation-and-transactions-in-consul/
 * <p>
 * Note: given implementation doesn't require to serialize/deserialize counter related data, instead it just manipulates plain strings.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulCounter extends ConsulMap<String, Long> implements Counter {

  // key to access counter.
  private final String consulKey;

  public ConsulCounter(String name, ClusterManagerInternalContext appContext) {
    super("__vertx.counters", appContext);
    this.consulKey = keyPath(name);
  }

  @Override
  public void get(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    getPlainValue(consulKey)
      .map(this::extractActualCounterValue)
      .setHandler(resultHandler);
  }

  @Override
  public void incrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    calculateAndCompareAndSwap(true, 1L, resultHandler);
  }

  @Override
  public void getAndIncrement(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    calculateAndCompareAndSwap(false, 1L, resultHandler);
  }

  @Override
  public void decrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    calculateAndCompareAndSwap(true, -1L, resultHandler);
  }

  @Override
  public void addAndGet(long value, Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    calculateAndCompareAndSwap(true, value, resultHandler);
  }

  @Override
  public void getAndAdd(long value, Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    calculateAndCompareAndSwap(false, value, resultHandler);
  }

  @Override
  public void compareAndSet(long expected, long value, Handler<AsyncResult<Boolean>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    getPlainValue(consulKey)
      .compose(keyValue -> {
        Promise<Boolean> promise = Promise.promise();
        final Long preValue = extractActualCounterValue(keyValue);
        if (preValue == expected) {
          putPlainValue(consulKey, String.valueOf(value), null).setHandler(promise);
        } else {
          promise.complete(false);
        }
        return promise.future();
      })
      .setHandler(resultHandler);
  }

  /**
   * Performs calculation operation on the counter.
   */
  private void calculateAndCompareAndSwap(boolean postGet, Long value, Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    getPlainValue(consulKey)
      .compose(keyValue -> {
        Promise<Long> result = Promise.promise();
        final Long preValue = extractActualCounterValue(keyValue);
        final Long postValue = preValue + value;
        putPlainValue(consulKey, String.valueOf(postValue), new KeyValueOptions().setCasIndex(keyValue.getModifyIndex()))
          .setHandler(putRes -> {
            if (putRes.succeeded()) {
              if (putRes.result()) {
                result.complete(postGet ? postValue : preValue);
              } else {
                // do retry until succeeded
                calculateAndCompareAndSwap(postGet, value, result);
              }
            } else {
              result.fail(putRes.cause());
            }
          });
        return result.future();
      })
      .setHandler(resultHandler);
  }

  /**
   * Extracts counter value (which is {@link Long}) out of {@link KeyValue}
   */
  private Long extractActualCounterValue(KeyValue keyValue) {
    return keyValue == null || keyValue.getValue() == null ? Long.valueOf(0L) : Long.valueOf(keyValue.getValue());
  }
}
