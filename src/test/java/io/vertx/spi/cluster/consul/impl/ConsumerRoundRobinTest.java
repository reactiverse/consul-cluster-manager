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
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.consul.ConsulCluster;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import io.vertx.test.core.VertxTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsumerRoundRobinTest extends VertxTestBase {

  private static final String MESSAGE_ADDRESS = "consumerAddress";

  private static int port;

  @BeforeClass
  public static void startConsulCluster() {
    port = ConsulCluster.init();
  }

  @AfterClass
  public static void shutDownConsulCluster() {
    ConsulCluster.shutDown();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(getClusterManagerOptions());
  }


  private CompletableFuture<Void> addConsumer(int index) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    vertices[0].eventBus().
      consumer(MESSAGE_ADDRESS, message -> message.reply(index)).
      completionHandler(event -> {
        if (event.succeeded()) {
          future.complete(null);
        } else {
          future.completeExceptionally(event.cause());
        }
      });
    return future;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    startNodes(1);
    CountDownLatch latch = new CountDownLatch(1);
    addConsumer(0).thenCompose(aVoid -> addConsumer(1)).thenCompose(aVoid -> addConsumer(2)).
      whenComplete((aVoid, throwable) -> {
        if (throwable != null) {
          fail(throwable);
        } else {
          latch.countDown();
        }
      });
    awaitLatch(latch);
  }

  @Test
  public void roundRobin() {
    AtomicInteger counter = new AtomicInteger(0);
    Set<Integer> results = new HashSet<>();
    Vertx vertx = vertices[0];
    vertx.setPeriodic(500, aLong -> vertx.eventBus().send(MESSAGE_ADDRESS, "Hi", message -> {
      if (message.failed()) {
        fail(message.cause());
      } else {
        Integer result = (Integer) message.result().body();
        results.add(result);
        if (counter.incrementAndGet() == 3) {
          assertEquals(results.size(), counter.get());
          testComplete();
        }
      }
    }));
    await();
  }

  private JsonObject getClusterManagerOptions() {
    return new JsonObject()
      .put("host", "localhost")
      .put("port", port);
  }

}
