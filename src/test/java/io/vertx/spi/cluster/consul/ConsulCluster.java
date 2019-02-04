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

import io.vertx.core.VertxException;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mock of consul cluster consisting of only one consul agent.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulCluster {

  private static ConsulAgent consulAgent;
  private static final AtomicBoolean started = new AtomicBoolean();

  public static int init() {
    if (started.compareAndSet(false, true)) {
      consulAgent = new ConsulAgent();
      return consulAgent.start();
    } else {
      throw new VertxException("Cluster has been already started!");
    }
  }

  public static void shutDown() {
    if (started.compareAndSet(true, false)) {
      consulAgent.stop();
    } else {
      throw new VertxException("Cluster has been already stopped!");
    }
  }
}
