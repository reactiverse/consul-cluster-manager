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
package io.vertx.core.eventbus;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.consul.ConsulCluster;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.instanceOf;

/**
 * Tests {@link io.vertx.core.eventbus.impl.clustered.ClusteredEventBus}
 * Caching is enabled in {@link io.vertx.spi.cluster.consul.impl.ConsulAsyncMultiMap}
 * <p>
 * Some tests contain explicit delay - this allows local caches to catch up with central consul kv store.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulApClusteredEventBusTest extends ClusteredEventBusTest {

  private static int port = 8500;

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
    JsonObject options = new JsonObject()
      .put("port", port)
      .put("host", "localhost")
      .put("preferConsistency", false);
    return new ConsulClusterManager(options);
  }

  /**
   * This test is a bit enhanced - contains 1.5 sec delays to let local cache catches up with consul KV.
   * Otherwise assertEquals(ReplyFailure.NO_HANDLERS, replyException.failureType()) will fail since
   * Handler will be found and TIMEOUT will take place.
   */
  @Test
  public void testSendWhileUnsubscribing() throws Exception {
    startNodes(2);
    AtomicBoolean unregistered = new AtomicBoolean();
    Verticle sender = new AbstractVerticle() {
      @Override
      public void start() throws Exception {
        sleep();
        getVertx().runOnContext(v -> sendMsg());
      }

      private void sendMsg() {
        if (!unregistered.get()) {
          getVertx().eventBus().send("whatever", "marseille");
          vertx.setTimer(1, id -> {
            sendMsg();
          });
        } else {
          getVertx().eventBus().send("whatever", "marseille", ar -> {
            Throwable cause = ar.cause();
            assertThat(cause, instanceOf(ReplyException.class));
            ReplyException replyException = (ReplyException) cause;
            assertEquals(ReplyFailure.NO_HANDLERS, replyException.failureType());
            testComplete();
          });
        }
      }
    };
    Verticle receiver = new AbstractVerticle() {
      boolean unregisterCalled;

      @Override
      public void start(Future<Void> startFuture) throws Exception {
        EventBus eventBus = getVertx().eventBus();
        MessageConsumer<String> consumer = eventBus.consumer("whatever");
        sleep();
        consumer.handler(m -> {
          if (!unregisterCalled) {
            consumer.unregister(v -> unregistered.set(true));
            unregisterCalled = true;
          }
          m.reply("ok");
        }).completionHandler(startFuture);
      }
    };
    CountDownLatch deployLatch = new CountDownLatch(1);
    vertices[0].exceptionHandler(this::fail).deployVerticle(receiver, onSuccess(receiverId -> {
      vertices[1].exceptionHandler(this::fail).deployVerticle(sender, onSuccess(senderId -> {
        deployLatch.countDown();
      }));
    }));
    awaitLatch(deployLatch);
    await();
    CountDownLatch closeLatch = new CountDownLatch(2);
    vertices[0].close(v -> closeLatch.countDown());
    vertices[1].close(v -> closeLatch.countDown());
    awaitLatch(closeLatch);
  }


  @Override
  protected <T> void testPublish(T val, Consumer<T> consumer) {
    int numNodes = 3;
    startNodes(numNodes);
    AtomicInteger count = new AtomicInteger();
    class MyHandler implements Handler<Message<T>> {
      @Override
      public void handle(Message<T> msg) {
        if (consumer == null) {
          assertFalse(msg.isSend());
          assertEquals(val, msg.body());
        } else {
          consumer.accept(msg.body());
        }
        if (count.incrementAndGet() == numNodes - 1) {
          testComplete();
        }
      }
    }
    AtomicInteger registerCount = new AtomicInteger(0);
    class MyRegisterHandler implements Handler<AsyncResult<Void>> {
      @Override
      public void handle(AsyncResult<Void> ar) {
        assertTrue(ar.succeeded());
        if (registerCount.incrementAndGet() == 2) {
          vertices[0].eventBus().publish(ADDRESS1, val);
        }
      }
    }
    MessageConsumer reg = vertices[2].eventBus().<T>consumer(ADDRESS1).handler(new MyHandler());
    reg.completionHandler(new MyRegisterHandler());
    reg = vertices[1].eventBus().<T>consumer(ADDRESS1).handler(new MyHandler());
    reg.completionHandler(new MyRegisterHandler());
    // let all vertices caches catch up with consul kv store.
    sleep();

    vertices[0].eventBus().publish(ADDRESS1, val);
    await();
  }

  private void sleep() {
    try {
      Thread.sleep(1500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
