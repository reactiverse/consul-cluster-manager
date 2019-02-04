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

import io.vertx.core.Vertx;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Consul cluster manager context.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public final class ClusterManagerInternalContext implements AutoCloseable {

  private String nodeId;
  private Vertx vertx;
  private ConsulClient consulClient;
  private ConsulClientOptions consulClientOptions;
  private String ephemeralSessionId; // consul session id used to make map entries ephemeral.
  // TODO: do we really need ConcurrentLinkedQueue?
  private Queue<Watch<KeyValueList>> watchQueue = new ConcurrentLinkedQueue<>();

  public ClusterManagerInternalContext setVertx(Vertx vertx) {
    checkIfInitialized(this.vertx, "vert.x");
    this.vertx = Objects.requireNonNull(vertx);
    return this;
  }

  public ClusterManagerInternalContext initConsulClient() {
    checkIfInitialized(consulClient, "consulClient");
    this.consulClient = ConsulClient.create(Objects.requireNonNull(vertx), Objects.requireNonNull(consulClientOptions));
    return this;
  }

  public ClusterManagerInternalContext setNodeId(String nodeId) {
    checkIfInitialized(this.nodeId, "nodeId");
    this.nodeId = Objects.requireNonNull(nodeId);
    return this;
  }

  public ClusterManagerInternalContext setConsulClientOptions(ConsulClientOptions consulClientOptions) {
    checkIfInitialized(this.consulClientOptions, "consulClientOptions");
    this.consulClientOptions = Objects.requireNonNull(consulClientOptions);
    return this;
  }

  public ClusterManagerInternalContext setEphemeralSessionId(String sessionId) {
    checkIfInitialized(this.ephemeralSessionId, "vert.x");
    this.ephemeralSessionId = Objects.requireNonNull(sessionId);
    return this;
  }

  public String getNodeId() {
    return nodeId;
  }

  public Vertx getVertx() {
    return vertx;
  }

  public ConsulClient getConsulClient() {
    return consulClient;
  }

  public ConsulClientOptions getConsulClientOptions() {
    return consulClientOptions;
  }

  public String getEphemeralSessionId() {
    return ephemeralSessionId;
  }

  Watch<KeyValueList> createAndGetWatch(String name) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(vertx);
    Objects.requireNonNull(consulClientOptions);
    Watch<KeyValueList> watch = Watch.keyPrefix(name, vertx, consulClientOptions);
    watchQueue.add(watch);
    return watch;
  }

  @Override
  public void close() {
    watchQueue.forEach(Watch::stop);
  }

  private <T> void checkIfInitialized(T instance, String instanceName) {
    if (instance != null) {
      throw new IllegalStateException(instanceName + " was already initialized!");
    }
  }
}
