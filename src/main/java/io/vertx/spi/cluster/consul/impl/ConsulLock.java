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

import io.vertx.core.Future;
import io.vertx.core.VertxException;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
import io.vertx.ext.consul.KeyValueOptions;

/**
 * Consul-based implementation of an asynchronous exclusive lock which can be obtained from any node in the cluster.
 * When the lock is obtained (acquired), no-one else in the cluster can acquire the lock with the same name until the lock is released.
 * <p>
 * <b> Given implementation is based on using consul sessions - see https://www.consul.io/docs/guides/leader-election.html.</b>
 * Some notes:
 * <p>
 * The state of our lock literally corresponds to the existence or non-existence of the respective key in the key-value store.
 * In order to acquire the lock we create a simple kv pair in Consul kv store and bound it with ttl session's id.
 * In order to release the lock we destroy respective session which triggers automatically the deleting of kv pair that was bound to it.
 * <p>
 * Some additional details:
 * https://github.com/hashicorp/consul/issues/432
 * https://blog.emilienkenler.com/2017/05/20/distributed-locking-with-consul/
 * <p>
 * Note: given implementation doesn't require to serialize/deserialize lock related data, instead it just manipulates plain strings.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulLock extends ConsulMap<String, String> implements Lock {

  private static final Logger log = LoggerFactory.getLogger(ConsulLock.class);

  private final String lockName;
  private final String sessionId;
  private final long timeout;

  /**
   * Creates an instance of consul based lock. MUST NOT be executed on the vertx event loop.
   *
   * @param name       - lock's name.
   * @param checkId    - check id to which session id will get bound to.
   * @param timeout    - time trying to acquire a lock in ms.
   * @param appContext - cluster manager appContext.
   */
  public ConsulLock(String name, String checkId, long timeout, ClusterManagerInternalContext appContext) {
    super("__vertx.locks", appContext);
    this.lockName = name;
    this.timeout = timeout;
    this.sessionId = getSessionId(checkId);
  }


  /**
   * Tries to acquire a lock in a blocking way.
   *
   * @return true - lock has been successfully obtained, false - otherwise.
   */
  public boolean tryToAcquire() {
    if (log.isDebugEnabled()) {
      log.debug("[" + appContext.getNodeId() + "]" + " is trying to acquire a lock on: " + lockName);
    }
    boolean lockAcquired = completeAndGet(acquire(), timeout);
    if (lockAcquired) {
      if (log.isDebugEnabled()) {
        log.debug("[" + appContext.getNodeId() + "] has acquired lock on: " + lockName);
      }
    }
    return lockAcquired;
  }

  @Override
  public void release() {
    destroySession(sessionId).setHandler(event -> {
      if (event.succeeded()) {
        if (log.isDebugEnabled()) {
          log.debug("[" + appContext.getNodeId() + "] has released lock on: " + lockName);
        }
      } else
        throw new VertxException("[" + appContext.getNodeId() + "] - failed to release a lock on: " + lockName + ". Lock might have been already released.", event.cause());
    });
  }

  /**
   * Obtains a session id from consul. IMPORTANT : lock MUST be bound to tcp check since failed (failing) node must give up any locks being held by it,
   * therefore obtained session id is being bounded to tcp check.
   *
   * @param checkId - tcp check id.
   * @return consul session id.
   */
  private String getSessionId(String checkId) {
    return completeAndGet(registerSession("Session for lock: " + lockName + " of: " + appContext.getNodeId(), checkId), timeout);
  }

  /**
   * Acquire the lock asynchronously.
   */
  private Future<Boolean> acquire() {
    return putPlainValue(keyPath(lockName), "lockAcquired", new KeyValueOptions().setAcquireSession(sessionId));
  }
}
