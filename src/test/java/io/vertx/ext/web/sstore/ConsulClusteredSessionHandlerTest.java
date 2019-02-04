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
package io.vertx.ext.web.sstore;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.consul.ConsulCluster;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import io.vertx.spi.cluster.consul.Utils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.function.Consumer;

/**
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulClusteredSessionHandlerTest extends ClusteredSessionHandlerTest {

  private final static int DEFAULT_PORT = Utils.getFreePort();
  private static int consulAgentPort = 8500;

  @BeforeClass
  public static void startConsulCluster() {
    consulAgentPort = ConsulCluster.init();
  }

  @AfterClass
  public static void shutDownConsulCluster() {
    ConsulCluster.shutDown();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(getClusterManagerOptions());
  }

  protected HttpServerOptions getHttpServerOptions() {
    return new HttpServerOptions().setPort(DEFAULT_PORT).setHost("localhost");
  }

  protected HttpClientOptions getHttpClientOptions() {
    return new HttpClientOptions().setDefaultPort(DEFAULT_PORT);
  }

  protected void testRequestBuffer(HttpMethod method, String path, Consumer<HttpClientRequest> requestAction, Consumer<HttpClientResponse> responseAction,
                                   int statusCode, String statusMessage,
                                   Buffer responseBodyBuffer, boolean normalizeLineEndings) throws Exception {
    testRequestBuffer(client, method, DEFAULT_PORT, path, requestAction, responseAction, statusCode, statusMessage, responseBodyBuffer, normalizeLineEndings);
  }

  private JsonObject getClusterManagerOptions() {
    return new JsonObject()
      .put("host", "localhost")
      .put("port", consulAgentPort);
  }

  @Override
  @Test
  @Ignore("Not supported")
  public void testRetryTimeout() throws Exception {
    super.testRetryTimeout();
  }
}
