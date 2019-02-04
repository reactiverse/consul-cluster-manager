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
package io.vertx;

import io.vertx.core.ConsulClusteredComplexHATest;
import io.vertx.core.ConsulClusteredHATest;
import io.vertx.core.eventbus.ConsulApClusteredEventBusTest;
import io.vertx.core.eventbus.ConsulCpClusteredEventBusTest;
import io.vertx.core.shareddata.*;
import io.vertx.ext.web.sstore.ConsulClusteredSessionHandlerTest;
import io.vertx.spi.cluster.consul.impl.ConsulNodeWithDefaultHostNameTest;
import io.vertx.spi.cluster.consul.impl.ConsulNodeWithSpecifiedHostNameTest;
import io.vertx.spi.cluster.consul.impl.ConsulSyncMapTest;
import io.vertx.spi.cluster.consul.impl.ConsumerRoundRobinTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Central test suite.
 * <p>
 * To enable slf4 logging specify this as VM options:
 * -ea -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory
 * <p>
 * Notes:
 * - FaultToleranceTest is not implemented.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  // HA
  ConsulClusteredHATest.class,
  ConsulClusteredComplexHATest.class,
  // SHARED DATA
  ConsulAsyncApMultiMapTest.class,
  ConsulAsyncCpMultiMapTest.class,
  ConsulClusteredAsyncMultiMapTest.class,
  ConsulClusteredAsyncMapTest.class,
  ConsulClusteredAsynchronousLockTest.class,
  ConsulClusteredSharedCounterTest.class,
  // SYNC MAP
  ConsulSyncMapTest.class,
  // ROUND ROBIN
  ConsumerRoundRobinTest.class,
  // EVENT BUS
  ConsulCpClusteredEventBusTest.class,
  ConsulApClusteredEventBusTest.class,
  ConsulClusteredSessionHandlerTest.class,
  // HOST
  ConsulNodeWithDefaultHostNameTest.class,
  ConsulNodeWithSpecifiedHostNameTest.class
})
public class ConsulClusterManagerTestSuite {
}
