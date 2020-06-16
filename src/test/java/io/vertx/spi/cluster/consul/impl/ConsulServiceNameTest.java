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
import io.vertx.ext.consul.*;
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
 * Covers https://github.com/reactiverse/consul-cluster-manager/issues/19.
 *
 * @author <a href="mailto:deedarb@gmail.com">Didar Burmaganov</a>
 */
public class ConsulServiceNameTest extends VertxTestBase {

  private static int consulAgentPort = 8500;
  private final String customServiceName = "customServiceName";
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
  public void testCustomServiceName() {
    externalConsulClient.catalogServices(serviceResultHandler -> {
      assertTrue(serviceResultHandler.succeeded());
      ServiceList result = serviceResultHandler.result();
      assertNotNull(result);
      List<Service> list = result.getList();
      assertNotNull(list);
      assertEquals(2, list.size());
      assertTrue(list.stream().anyMatch(service -> service.getName().equals(customServiceName)));

      complete();
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
      .put("serviceName", customServiceName)
      .put("host", "localhost")
      .put("port", consulAgentPort);
  }
}
