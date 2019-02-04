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

import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulAsyncBaseMultiMapTest extends AsyncMultiMapTest {

  protected volatile AsyncMultiMap<String, ClusterNodeInfo> clusterNodeIdMap;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    CountDownLatch latch = new CountDownLatch(1);
    clusterManager.<String, ClusterNodeInfo>getAsyncMultiMap("clusterNodeIdMap", onSuccess(res -> {
      clusterNodeIdMap = res;
      latch.countDown();
    }));
    awaitLatch(latch);
  }

  // -- ADDITIONAL TESTS --//

  @Test
  public void testAddAndGetClusterNodeInfo() {
    ClusterNodeInfo clusterNodeAInfo = new ClusterNodeInfo(clusterManager.getNodeID(), new ServerID(8080, "localhost"));
    ClusterNodeInfo clusterNodeBInfo = new ClusterNodeInfo(clusterManager.getNodeID(), new ServerID(8081, "localhost"));
    String address = "testAddAndGetClusterNodeInfo";

    clusterNodeIdMap.add(address, clusterNodeAInfo, cHandler_1 -> {
      assertTrue(cHandler_1.succeeded());
      clusterNodeIdMap.add(address, clusterNodeBInfo, cHandler_2 -> {
        assertTrue(cHandler_2.succeeded());

        clusterNodeIdMap.get(address, resultHandler -> {
          assertTrue(resultHandler.succeeded());
          Set<ClusterNodeInfo> expectedNodeSet = Stream.of(clusterNodeAInfo, clusterNodeBInfo).collect(Collectors.toSet());
          ChoosableIterable<ClusterNodeInfo> result = resultHandler.result();
          result.forEach(clusterNodeInfo -> assertTrue(expectedNodeSet.contains(clusterNodeAInfo)));
          testComplete();
        });

      });
    });
    await();
  }

  @Test
  public void testRemoveClusterNodeInfo() {
    ClusterNodeInfo clusterNodeAInfo = new ClusterNodeInfo(clusterManager.getNodeID(), new ServerID(8080, "localhost"));
    ClusterNodeInfo clusterNodeBInfo = new ClusterNodeInfo(clusterManager.getNodeID(), new ServerID(8081, "localhost"));
    String address = "testRemoveClusterNodeInfo";

    clusterNodeIdMap.add(address, clusterNodeAInfo, cHandler_1 -> {
      assertTrue(cHandler_1.succeeded());
      clusterNodeIdMap.add(address, clusterNodeBInfo, cHandler_2 -> {
        assertTrue(cHandler_2.succeeded());
        ClusterNodeInfo notExistingClusterNodeInfo = new ClusterNodeInfo(UUID.randomUUID().toString(), new ServerID(1111, "localhost"));
        clusterNodeIdMap.remove(address, notExistingClusterNodeInfo, cHandler_3 -> {
          assertTrue(cHandler_3.succeeded());
          assertFalse(cHandler_3.result());
          clusterNodeIdMap.get(address, rHandler_1 -> {
            assertTrue(rHandler_1.succeeded());
            Set<ClusterNodeInfo> expectedNodeSet = Stream.of(clusterNodeAInfo, clusterNodeBInfo).collect(Collectors.toSet());
            rHandler_1.result().forEach(clusterNodeInfo -> assertTrue(expectedNodeSet.contains(clusterNodeAInfo)));
            clusterNodeIdMap.remove(address, clusterNodeBInfo, cHandler_4 -> {
              assertTrue(cHandler_4.succeeded());
              assertTrue(cHandler_4.result());
              clusterNodeIdMap.get(address, rHandler_2 -> {
                assertTrue(rHandler_2.succeeded());
                List<ClusterNodeInfo> receivedSet = new ArrayList<>();
                rHandler_2.result().forEach(clusterNodeInfo -> receivedSet.add(clusterNodeAInfo));
                assertEquals(receivedSet.get(0), clusterNodeAInfo);
                testComplete();
              });
            });
          });
        });
      });
    });
    await();
  }

  @Test
  public void testRemoveAllForValueClusterNodeInfo() {
    String users = "users.removeAllForValue";
    ClusterNodeInfo usersNodeASub = new ClusterNodeInfo(clusterManager.getNodeID(), new ServerID(8080, "192.168.0.1"));
    ClusterNodeInfo usersNodeBSub = new ClusterNodeInfo(clusterManager.getNodeID(), new ServerID(8081, "192.168.0.2"));
    String posts = "posts.removeAllForValue";
    ClusterNodeInfo postsNodeASub = new ClusterNodeInfo(clusterManager.getNodeID(), new ServerID(8080, "192.168.0.1"));
    ClusterNodeInfo postsNodeBSub = new ClusterNodeInfo(clusterManager.getNodeID(), new ServerID(8081, "192.168.0.2"));

    clusterNodeIdMap.add(users, usersNodeASub, handler_1 -> {
      assertTrue(handler_1.succeeded());
      clusterNodeIdMap.add(users, usersNodeBSub, handler_2 -> {
        assertTrue(handler_2.succeeded());
        clusterNodeIdMap.add(posts, postsNodeASub, handler_3 -> {
          assertTrue(handler_3.succeeded());
          clusterNodeIdMap.add(posts, postsNodeBSub, handler_4 -> {
            assertTrue(handler_4.succeeded());
            clusterNodeIdMap.removeAllForValue(usersNodeASub, rHandler -> {
              assertTrue(rHandler.succeeded());
              clusterNodeIdMap.get(users, usersSubs -> {
                assertTrue(usersSubs.succeeded());
                // usersNodeASub was hopefully removed.
                Set<ClusterNodeInfo> expectedNodeSet = Stream.of(usersNodeBSub).collect(Collectors.toSet());
                usersSubs.result().forEach(clusterNodeInfo -> assertTrue(expectedNodeSet.contains(clusterNodeInfo)));
                clusterNodeIdMap.get(posts, postsSubs -> {
                  assertTrue(postsSubs.succeeded());
                  Set<ClusterNodeInfo> expectedNodeBSet = Stream.of(postsNodeBSub).collect(Collectors.toSet());
                  postsSubs.result().forEach(clusterNodeInfo -> assertTrue(expectedNodeBSet.contains(clusterNodeInfo)));
                  testComplete();
                });
              });
            });
          });
        });
      });
    });
    await();
  }
}
