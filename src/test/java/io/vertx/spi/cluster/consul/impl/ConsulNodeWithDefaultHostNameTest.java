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

import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.Check;
import io.vertx.ext.consul.CheckStatus;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulCluster;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import io.vertx.test.core.VertxTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Covers https://github.com/romalev/vertx-consul-cluster-manager/issues/92.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulNodeWithDefaultHostNameTest extends VertxTestBase {

  private static int consulAgentPort = 8500;

  private ConsulClient externalConsulClient;

  @BeforeClass
  public static void startConsulCluster() {
    consulAgentPort = ConsulCluster.init();
  }

  @AfterClass
  public static void shutDownConsulCluster() {
    ConsulCluster.shutDown();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    startNodes(1);
    externalConsulClient = ConsulClient.create(vertx, new ConsulClientOptions(getConfig()));
  }

  @Test
  public void testDefaultNodeHostAddress() {
    externalConsulClient.getValues("__vertx.nodes", nodesResultHandler -> {
      assertTrue(nodesResultHandler.succeeded());
      assertNotNull(nodesResultHandler.result());
      assertNotNull(nodesResultHandler.result().getList());
      assertEquals(1, nodesResultHandler.result().getList().size());
      assertTrue(nodesResultHandler.result().getList().get(0).getValue() != null);
      assertFalse(nodesResultHandler.result().getList().get(0).getValue().isEmpty());
      JsonObject nodeAddress = new JsonObject(nodesResultHandler.result().getList().get(0).getValue());
      try {
        assertEquals(InetAddress.getLocalHost().getHostAddress(), nodeAddress.getString("host"));
      } catch (UnknownHostException e) {
        fail(e);
      }
      externalConsulClient.healthChecks("vert.x-cluster-manager", checksResultHandler -> {
        assertTrue(checksResultHandler.succeeded());
        assertNotNull(checksResultHandler.result());
        List<Check> checkList = checksResultHandler.result().getList();
        assertNotNull(checkList);
        assertEquals(1, checkList.size());
        assertEquals(CheckStatus.PASSING, checkList.get(0).getStatus());
        complete();
      });
    });
    await();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(getConfig());
  }

  @Override
  public void after() throws Exception {
    super.after();
    externalConsulClient.close();
  }

  // no nodeHost is specified -> default one will be used i.e. InetAddress.getLocalHost().getHostAddress() will get executed.
  private JsonObject getConfig() {
    return new JsonObject()
      .put("host", "localhost")
      .put("port", consulAgentPort);
  }
}
