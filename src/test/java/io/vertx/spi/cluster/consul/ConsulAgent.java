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
package io.vertx.spi.cluster.consul;

import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Embedded consul agent.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulAgent {
  private final Logger log = LoggerFactory.getLogger(ConsulAgent.class);

  private int port;
  private ConsulProcess consul;

  ConsulAgent() {
    this.port = Utils.getFreePort();
  }

  int start() {
    consul = ConsulStarterBuilder.consulStarter().withHttpPort(port).build().start();
    log.info("Consul test agent is up and running on port: " + port);
    return port;
  }

  void stop() {
    consul.close();
    log.info("Consul test agent has been stopped.");
  }

  public int getPort() {
    return port;
  }


}
