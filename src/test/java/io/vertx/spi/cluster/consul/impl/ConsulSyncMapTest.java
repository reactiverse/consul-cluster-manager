/*
 * Copyright (C) 2018-2019-2019 Roman Levytskyi
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

import io.vertx.core.Vertx;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.SessionBehavior;
import io.vertx.ext.consul.SessionOptions;
import io.vertx.spi.cluster.consul.ConsulCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Test for {@link ConsulSyncMap}
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulSyncMapTest {

  private ClusterManagerInternalContext cmContext;
  private int port;

  @Before
  public void setUp() {
    cmContext = new ClusterManagerInternalContext();
    port = ConsulCluster.init();
    ConsulClientOptions options = new ConsulClientOptions()
      .setPort(port)
      .setHost("localhost");
    cmContext.setVertx(Vertx.vertx())
      .setConsulClientOptions(options)
      .initConsulClient()
      .setNodeId("testSyncMapNodeId")
      .setEphemeralSessionId(getSessionId());
  }

  @Test
  public void syncMapOperation() {
    String k = "myKey";
    String v = "myValue";

    ConsulSyncMap<String, String> syncMap = new ConsulSyncMap<>("syncMapTest", cmContext);

    syncMap.put(k, v);
    assertFalse(syncMap.isEmpty());

    assertEquals(syncMap.get(k), v);

    assertTrue(syncMap.size() > 0);
    assertTrue(syncMap.containsKey(k));
    assertTrue(syncMap.containsValue(v));

    assertTrue(syncMap.keySet().contains(k));
    assertTrue(syncMap.values().contains(v));

    syncMap.entrySet().forEach(entry -> {
      assertEquals(k, entry.getKey());
      assertEquals(v, entry.getValue());
    });

    String value = syncMap.remove(k);
    assertEquals(value, v);
    assertNull(syncMap.get(k));

    syncMap.clear();
    assertTrue(syncMap.isEmpty());

  }

  @After
  public void tearDown() {
    destroySessionId();
    cmContext.getVertx().close();
    cmContext.getConsulClient().close();
    ConsulCluster.shutDown();
  }

  private String getSessionId() {
    CompletableFuture<String> future = new CompletableFuture<>();
    SessionOptions sessionOptions = new SessionOptions()
      .setBehavior(SessionBehavior.DELETE)
      .setLockDelay(0) // can't specify 0 - perhaps bug in consul client implementation.
      .setName("test");
    cmContext.getConsulClient().createSessionWithOptions(sessionOptions, resultHandler -> {
      if (resultHandler.succeeded()) future.complete(resultHandler.result());
      else future.completeExceptionally(resultHandler.cause());
    });

    String sId = null;
    try {
      sId = future.get(2000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      fail(e.getMessage());
    }
    return sId;
  }

  private void destroySessionId() {
    CountDownLatch latch = new CountDownLatch(1);
    cmContext.getConsulClient().destroySession(cmContext.getEphemeralSessionId(), event -> latch.countDown());
    try {
      latch.await(2000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }
  }
}
