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

import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.consul.ConsulCluster;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import io.vertx.spi.cluster.consul.impl.ChoosableSet;
import io.vertx.spi.cluster.consul.impl.ConsulAsyncMultiMap;
import io.vertx.test.core.VertxTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulClusteredAsyncMultiMapTest extends VertxTestBase {

  private ClusterManager[] clusterManagers;
  private volatile AsyncMultiMap<String, ServerID>[] maps;


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
  public void setUp() throws Exception {
    super.setUp();
    startNodes(2);
    clusterManagers = new ClusterManager[2];
    maps = new ConsulAsyncMultiMap[2];
    clusterManagers[0] = ((VertxInternal) vertices[0]).getClusterManager();
    clusterManagers[1] = ((VertxInternal) vertices[1]).getClusterManager();
    CountDownLatch latch = new CountDownLatch(2);
    clusterManagers[0].<String, ServerID>getAsyncMultiMap("subs", onSuccess(res -> {
      maps[0] = res;
      latch.countDown();
    }));
    clusterManagers[1].<String, ServerID>getAsyncMultiMap("subs", onSuccess(res -> {
      maps[1] = res;
      latch.countDown();
    }));
    awaitLatch(latch);
  }

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(getConsulClusterManagerOptions());
  }

  @Test
  public void testAddAndGet() {
    String addressLviv = "Lviv";
    String addressCherche = "LvivCherche";
    maps[0].add(addressLviv, new ServerID(10, "localhost"), res_10 -> {
      assertTrue(res_10.succeeded());
      maps[0].add(addressCherche, new ServerID(11, "localhost"), res_11 -> {
        assertTrue(res_11.succeeded());
        maps[0].get(addressLviv, res_20 -> {
          assertTrue(res_20.succeeded());
          ChoosableSet<ServerID> result_20 = (ChoosableSet<ServerID>) res_20.result();
          assertNotNull(result_20);
          assertEquals(1, result_20.size());
          maps[1].get(addressLviv, res_3 -> {
            assertTrue(res_3.succeeded());
            ChoosableSet<ServerID> result_30 = (ChoosableSet<ServerID>) res_3.result();
            assertNotNull(result_30);
            assertEquals(1, result_30.size());
            testComplete();
          });
        });
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
