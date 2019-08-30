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
package io.vertx.core.shareddata;

import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulCluster;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * Tests for {@link io.vertx.spi.cluster.consul.impl.ConsulAsyncMap}
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulClusteredAsyncMapTest extends ClusteredAsyncMapTest {

  private ConsulClient consulClient;
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
    return new ConsulClusterManager(getConsulClusterManagerOptions());
  }

  @Override
  public void before() throws Exception {
    super.before();
    if (consulClient == null) {
      consulClient = ConsulClient.create(vertx, new ConsulClientOptions(getConsulClusterManagerOptions()));
    }
  }

  @Override
  public void after() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);
    consulClient.deleteValues("foo", event -> latch.countDown());
    consulClient.deleteValues("bar", event -> latch.countDown());
    latch.await();
    super.after();
  }

  // we don't support keys containing double slashes and fully really on consul - https://github.com/hashicorp/consul/issues/3476
  @Test
  public void testMapPutGetDoubleSlash() {
    getVertx().sharedData().getAsyncMap("foo", asyncMapHandler -> {
      assertTrue(asyncMapHandler.succeeded());
      assertNotNull(asyncMapHandler.result());
      asyncMapHandler.result().put("//key", "value", handler -> {
        assertTrue(handler.failed());
        assertTrue(handler.cause().getMessage().contains("Moved Permanently"));
        complete();
      });
    });
    await();
  }

  private JsonObject getConsulClusterManagerOptions() {
    return new JsonObject()
      .put("host", "localhost")
      .put("port", port);
  }
}
