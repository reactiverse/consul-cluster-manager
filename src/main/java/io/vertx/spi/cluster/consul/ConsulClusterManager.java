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

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.CheckList;
import io.vertx.ext.consul.CheckOptions;
import io.vertx.ext.consul.CheckStatus;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.KeyValueOptions;
import io.vertx.ext.consul.ServiceOptions;
import io.vertx.spi.cluster.consul.impl.ClusterManagerInternalContext;
import io.vertx.spi.cluster.consul.impl.ConsulAsyncMap;
import io.vertx.spi.cluster.consul.impl.ConsulAsyncMultiMap;
import io.vertx.spi.cluster.consul.impl.ConsulCounter;
import io.vertx.spi.cluster.consul.impl.ConsulLock;
import io.vertx.spi.cluster.consul.impl.ConsulMap;
import io.vertx.spi.cluster.consul.impl.ConsulSyncMap;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Cluster manager that uses Consul. Given implementation is based on vertx consul client.
 * Current restrictions :
 * <p>
 * - The limit on a key's value size of any of the consul maps is 512KB. This is strictly enforced and an HTTP 413 status will be returned to
 * any client that attempts to store more than that limit in a value. It should be noted that the Consul key/value store is not designed to be used as a general purpose database.
 * - Compatible with vert.x 3.6.0+ release due to:
 * -- https://github.com/vert-x3/vertx-consul-client/issues/54
 * -- https://github.com/vert-x3/vertx-consul-client/issues/56
 * -- {@link io.vertx.core.impl.HAManager} enhancements -> sync map operations are always executed on the thread from the worker pool.
 * <p>
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulClusterManager extends ConsulMap<String, String> implements ClusterManager {

  private static final Logger log = LoggerFactory.getLogger(ConsulClusterManager.class);
  private static final String HA_INFO_MAP_NAME = "__vertx.haInfo";
  private static final String NODES_MAP_NAME = "__vertx.nodes";
  private static final String SERVICE_NAME = "vert.x-cluster-manager";
  private static final long TCP_CHECK_INTERVAL = 10_000;
  /*
   * We have to lock the "node joining the cluster" and "node leaving the cluster" operations
   * to stay as much as possible transactionally in sync with consul kv store.
   * Only one node joins the cluster at particular point in time. This is achieved through acquiring a distributed lock.
   * Given lock is held until the node itself receives an appropriate "NODE JOINED" or ("NODE_LEFT") event through consul watch.
   * Having this allow us to ensure :
   * - nodeAdded and nodeRemoved are called on the nodeListener in the same order on the same nodes with same node ids.
   * - getNodes() always return same node list when nodeAdded and nodeRemoved are called on the nodeListener.
   * Same happens with node leaving the cluster.
   * Without locking we'd screw up an entire HA.
   *
   * Note: question was raised in consul google groups: https://groups.google.com/forum/#!topic/consul-tool/A0yJV0EKclw
   * so far no answers.
   */
  private final static String NODE_JOINING_LOCK_NAME = "nodeJoining";
  private final static String NODE_LEAVING_LOCK_NAME = "nodeLeaving";
  private final Map<String, Lock> locks = new ConcurrentHashMap<>();
  private final Map<String, Counter> counters = new ConcurrentHashMap<>();
  private final Map<String, Map<?, ?>> syncMaps = new ConcurrentHashMap<>();
  private final Map<String, AsyncMap<?, ?>> asyncMaps = new ConcurrentHashMap<>();
  private final Map<String, AsyncMultiMap<?, ?>> asyncMultiMaps = new ConcurrentHashMap<>();
  private final JsonObject consulClusterManagerConfig;
  /*
   * A set that attempts to keep all cluster node's data locally cached. Cluster manager
   * watches the consul "__vertx.nodes" path, responds to update/create/delete events, pull down the data.
   */
  private final Set<String> nodes = new HashSet<>();
  private final AtomicReference<Lock> nodeJoiningLock = new AtomicReference<>();
  private final AtomicReference<Lock> nodeLeavingLock = new AtomicReference<>();

  private final TaskQueue taskQueue = new TaskQueue();
  /*
   * Famous CAP theorem takes place here.
   * * CP - we trade consistency against availability,
   * * AP - we trade availability (latency) against consistency.
   * Given cluster management SPI implementation uses internal caching of consul KV stores (pure @{link ConcurrentHashMap} acts as cache and
   * caching is implemented on top consul watches.)
   *
   * Scenario: imagine we have nodes A and B in our cluster. A puts entry X to Consul KV and gets entry X cached locally.
   * B receives ENTRY_ADDED event (which is a X entry) and caches it locally accordingly.
   * Now imagine A removes an entry X from Consul KV and in one nanosecond later (right after remove of entry X has been acknowledged) node B queries entry X
   * and … well it receives an entry X from local cache since local node B cache became inconsistent with Consul KV since it has not yet received and processed EVENT_REMOVED event.
   * In order make local caching strongly consistent with Consul KV we have to check if data that is present in local cache is the same as data that is present in Consul KV.
   *
   * To enable dirty reads (and prefer having better latency) set {@code preferConsistency} flag to FALSE.
   * Otherwise set {@code preferConsistency} flag to TRUE to enable stronger consistency level (i.e. disable internal caching and use consul kv store directly).
   */
  private final boolean preferConsistency;

  private String checkId; // tcp consul check id
  private NetServer tcpServer; // dummy TCP server to receive and acknowledge heart beats messages from consul.
  private JsonObject nodeTcpAddress = new JsonObject(); // node's tcp address.
  private volatile boolean active; // identifies whether cluster manager is active or passive.
  private NodeListener nodeListener;

  /**
   * Creates consul cluster manager instance with specified config.
   * Example:
   * <pre>
   * JsonObject options = new JsonObject()
   *  .put("host", "consulAgentHost")
   *  .put("port", consulAgentPort)
   *  .put("preferConsistency", true);
   * </pre>
   * If config key is not specified -> default configuration takes place:
   * host -> localhost; port - 8500; preferConsistency - false
   *
   * @param config holds configuration for consul cluster manager client.
   */
  public ConsulClusterManager(final JsonObject config) {
    super(NODES_MAP_NAME, new ClusterManagerInternalContext());
    this.consulClusterManagerConfig = config;
    Objects.requireNonNull(config, "Given cluster manager can't get initialized.");
    appContext
      .setConsulClientOptions(new ConsulClientOptions(config))
      .setNodeId(UUID.randomUUID().toString());
    this.checkId = appContext.getNodeId();
    this.preferConsistency = config.containsKey("preferConsistency") ? config.getBoolean("preferConsistency") : false;
  }

  /**
   * Creates consul cluster manager instance with default configurations:
   * host -> localhost; port - 8500; preferConsistency - false
   */
  public ConsulClusterManager() {
    this(new JsonObject());
  }

  @Override
  public void setVertx(Vertx vertx) {
    appContext.setVertx(vertx);
  }

  @Override
  public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> asyncResultHandler) {
    Promise<AsyncMultiMap<K, V>> promiseAsyncMultiMap = Promise.promise();
    AsyncMultiMap asyncMultiMap = asyncMultiMaps.computeIfAbsent(name, key -> new ConsulAsyncMultiMap<>(name, preferConsistency, appContext));
    promiseAsyncMultiMap.complete(asyncMultiMap);
    promiseAsyncMultiMap.future().setHandler(asyncResultHandler);
  }

  @Override
  public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {
    Promise<AsyncMap<K, V>> promiseAsyncMap = Promise.promise();
    AsyncMap asyncMap = asyncMaps.computeIfAbsent(name, key -> new ConsulAsyncMap<>(name, appContext, this));
    promiseAsyncMap.complete(asyncMap);
    promiseAsyncMap.future().setHandler(asyncResultHandler);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return (Map<K, V>) syncMaps.computeIfAbsent(name, key -> new ConsulSyncMap<>(name, appContext));
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
    appContext.getVertx().executeBlocking(futureLock -> {
      ConsulLock lock = new ConsulLock(name, checkId, timeout, appContext);
      boolean lockAcquired = false;
      long remaining = timeout;
      do {
        long start = System.nanoTime();
        try {
          lockAcquired = lock.tryToAcquire();
        } catch (VertxException e) {
          // OK continue
        }
        remaining = remaining - MILLISECONDS.convert(System.nanoTime() - start, NANOSECONDS);
      } while (!lockAcquired && remaining > 0);
      if (lockAcquired) {
        locks.put(name, lock);
        futureLock.complete(lock);
      } else {
        log.warn("[" + appContext.getNodeId() + "]: timed out to get a lock on: " + name);
        futureLock.fail("Timed out waiting to get lock on: " + name);
      }
    }, false, resultHandler);
  }

  @Override
  public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
    Objects.requireNonNull(name);
    Promise<Counter> counterFuture = Promise.promise();
    Counter counter = counters.computeIfAbsent(name, key -> new ConsulCounter(name, appContext));
    counterFuture.complete(counter);
    counterFuture.future().setHandler(resultHandler);
  }

  @Override
  public String getNodeID() {
    return appContext.getNodeId();
  }

  @Override
  public List<String> getNodes() {
    return new ArrayList<>(nodes);
  }

  @Override
  public void nodeListener(NodeListener listener) {
    this.nodeListener = listener;
  }

  /**
   * For new node to join the cluster we perform:
   * <p>
   * - We create {@link ConsulClient} instance and save it to internal {@link ClusterManagerInternalContext}
   * <p>
   * - We register consul vert.x cluster management service in consul agent. Note: only one CM service gets created.
   * <p>
   * - We create dummy TCP server to able to receive and acknowledge heart beats messages from consul.
   * <p>
   * - We create TCP check (and get it registered within consul agent) to let consul agent sends heart beats messages to previously mentioned tcp server.
   * This allows consul agent to be aware of what is going on within the cluster: node is active if it acknowledges hear beat message, inactive - otherwise.
   * {@code TCP_CHECK_INTERVAL} holds actual interval for which consul agent will be sending heart beat messages to vert.x nodes.
   * <p>
   * - We create session in consul agent. Session's id is used later on to make consul map entries ephemeral (every entry that gets created with special {@link KeyValueOptions} holding this session id gets automatically deleted
   * from the consul cluster once session gets invalidated).
   * In this cluster manager case session will get invalidated when:
   * <li>health check gets unregistered.</li>
   * <li>health check falls into critical state {@link CheckStatus} - this happens when our dummy TCP server doesn't acknowledge the consul's heartbeat message).</li>
   * <li>session is explicitly destroyed.</li>
   * <p>
   * - Once all the steps above succeeded we start a consul watch on "__vertx.nodes" to capture all the details about nodes that are already part of the cluster.
   * - We try to clean HA_INFO map if cluster is empty (HA_INFO entries are NOT ephemeral -> they have to cleaned explicitly).
   * - We add the node (the one that called the join method) to the cluster by applying distributed locking. See {@code nodeJoiningLock} and {@code nodeLeavingLock}.
   * (We consider the node has "successfully" joined the cluster in case:
   * -- an appropriate entry has been placed under "__vertx.nodes" map
   * AND
   * -- consul watch pushed an appropriate nodeAdded event which has "successfully" been received by already mentioned node.
   * We consider the node has "successfully" left the cluster if:
   * -- an appropriate entry (that represents the node) has been removed from "__vertx.nodes" map
   * AND
   * -- consul watch pushed an appropriate nodeLeft event which has "successfully" been received by already mentioned node.
   * )
   * <p>
   * If any of the steps above fails we perform some "clean up".
   */
  @Override
  public synchronized void join(Handler<AsyncResult<Void>> resultHandler) {
    log.debug(appContext.getNodeId() + " is trying to join the cluster.");
    if (!active) {
      active = true;
      appContext.initConsulClient();
      deregisterFailingTcpChecks()
        .compose(aVoid -> createTcpServer())
        .compose(aVoid -> registerService())
        .compose(aVoid -> registerTcpCheck())
        .compose(aVoid -> registerSessionAndSave())
        .compose(aVoid -> {
          startListening(); // start a watch to listen for an updates on _vertx.nodes.
          return clearHaInfoMap();
        })
        .compose(aVoid -> addLocalNode(nodeTcpAddress))
        .setHandler(nodeJoinedEvent -> {
          if (nodeJoinedEvent.succeeded())
            resultHandler.handle(succeededFuture());
          else {
            // undo - if node can't join the cluster then:
            try {
              shutdownTcpServer();
              // this will trigger the remove of all ephemeral entries
              if (Objects.nonNull(appContext.getEphemeralSessionId()))
                destroySession(appContext.getEphemeralSessionId());
              deregisterTcpCheck();
              appContext.close();
            } finally {
              resultHandler.handle(failedFuture(nodeJoinedEvent.cause()));
            }
          }
        });
    } else {
      log.warn(appContext.getNodeId() + " is NOT active.");
      resultHandler.handle(succeededFuture());
    }
  }

  /**
   * This describers what happens when existing node leaves the cluster.
   */
  @Override
  public synchronized void leave(Handler<AsyncResult<Void>> resultHandler) {
    log.debug(appContext.getNodeId() + " is trying to leave the cluster.");
    if (active) {
      active = false;
      // forcibly release all lock being held by node.
      locks.values().forEach(Lock::release);
      removeLocalNode()
        .compose(aVoid -> destroySession(appContext.getEphemeralSessionId()))
        .compose(aVoid -> deregisterTcpCheck())
        .compose(aVoid -> shutdownTcpServer())
        .compose(aVoid -> {
          appContext.close();
          log.debug("[" + appContext.getNodeId() + "] has left the cluster.");
          return Future.<Void>succeededFuture();
        })
        .setHandler(resultHandler);
    } else {
      log.warn(appContext.getNodeId() + "' is NOT active.");
      resultHandler.handle(Future.succeededFuture());
    }
  }

  @Override
  public boolean isActive() {
    return active;
  }


  /**
   * Gets the session registered in consul agent and if this successes -> get it saved within {@link ClusterManagerInternalContext}.
   */
  private Future<Void> registerSessionAndSave() {
    return registerSession("Session for ephemeral keys for: " + appContext.getNodeId(), checkId)
      .compose(s -> {
        appContext.setEphemeralSessionId(s);
        return succeededFuture();
      });
  }

  /**
   * Adds local node (the one that uses this cluster manager) to the cluster (new entry gets created within {@code NODES_MAP_NAME} where key is node id).
   *
   * @param details - IP address of node.
   */
  private Future<Void> addLocalNode(JsonObject details) {
    Promise<Lock> lockPromise = Promise.promise();
    /*
     * Time to fight for a "node_joining" lock should exceed @{code TCP_CHECK_INTERVAL} - this is done in purpose for the situation
     * where node is the middle of joining the cluster (new entry has been placed under __vertx.nodes map)
     * but the node wasn't able to release "node_joining" lock (watch didn't push the nodeAdded event,
     * or it pushed but the event was lost somewhere on the wire)
     * In this case given node is not considered "healthy" and gets removed by consul agent automatically as well as its lock (due to having ephemeral session id being placed).
     * This gives another node a chance to acquire "node_joining" lock.
     */
    long timeout = TCP_CHECK_INTERVAL + 100;
    getLockWithTimeout(NODE_JOINING_LOCK_NAME, timeout, lockPromise);
    return lockPromise.future().compose(aLock -> {
      nodeJoiningLock.set(aLock);
      return putPlainValue(
        keyPath(appContext.getNodeId()),
        details.encode(),
        new KeyValueOptions().setAcquireSession(appContext.getEphemeralSessionId())
      );
    }).compose(nodeAdded -> {
      if (nodeAdded) return Future.succeededFuture();
      else {
        nodeJoiningLock.get().release();
        return Future.failedFuture("Node: " + appContext.getNodeId() + "failed to join the cluster.");
      }
    });
  }

  /**
   * Removes local node (the one that uses this cluster manager) from the cluster (existing entry gets removed from {@code NODES_MAP_NAME}).
   */
  private Future<Void> removeLocalNode() {
    Promise<Lock> lockPromise = Promise.promise();
    long timeout = TCP_CHECK_INTERVAL + 100;
    getLockWithTimeout(NODE_LEAVING_LOCK_NAME, timeout, lockPromise);
    return lockPromise.future().compose(aLock -> {
      nodeLeavingLock.set(aLock);
      return deleteValueByKeyPath(keyPath(appContext.getNodeId()));
    }).compose(nodeRemoved -> {
      if (nodeRemoved) return Future.succeededFuture();
      else {
        nodeLeavingLock.get().release();
        return Future.failedFuture("Node: " + appContext.getNodeId() + "failed to leave the cluster.");
      }
    });
  }

  /**
   * Checks cluster on emptiness.
   */
  private Future<Boolean> isClusterEmpty() {
    return plainKeys().compose(clusterNodes -> Future.succeededFuture(clusterNodes.isEmpty()));
  }

  /**
   * Clears out an entire HA_INFO map in case cluster is empty.
   */
  private Future<Void> clearHaInfoMap() {
    return isClusterEmpty().compose(clusterEmpty -> {
      if (clusterEmpty) {
        Promise<Void> clearHaInfoPromise = Promise.promise();
        ((ConsulSyncMap) getSyncMap(HA_INFO_MAP_NAME)).clear(handler -> clearHaInfoPromise.complete());
        return clearHaInfoPromise.future();
      } else {
        return Future.succeededFuture();
      }
    });
  }

  @Override
  public void entryUpdated(EntryEvent event) {
    String receivedNodeId = actualKey(event.getEntry().getKey());
    switch (event.getEventType()) {
      case WRITE: {
        boolean added = nodes.add(receivedNodeId);
        if (log.isTraceEnabled() && added) {
          log.trace("[" + appContext.getNodeId() + "]" + " New node: " + receivedNodeId + " has joined the cluster.");
        }
        if (receivedNodeId.equals(appContext.getNodeId())) {
          nodeJoiningLock.get().release();
        }
        if (nodeListener != null && active) {
          VertxInternal vertxInternal = (VertxInternal) appContext.getVertx();
          vertxInternal.getOrCreateContext().executeBlocking(runOnWorkingPool -> {
            nodeListener.nodeAdded(receivedNodeId);
            runOnWorkingPool.complete();
          }, taskQueue, res -> {
            if (log.isTraceEnabled()) {
              log.trace("[" + appContext.getNodeId() + "]" + " Node: " + receivedNodeId + " has been added to nodeListener.", receivedNodeId);
            }
          });
        }
        break;
      }
      case REMOVE: {
        boolean removed = nodes.remove(receivedNodeId);
        if (log.isTraceEnabled() && removed) {
          log.trace("[" + appContext.getNodeId() + "]" + " Node: " + receivedNodeId + " has left the cluster.");
        }
        if (receivedNodeId.equals(appContext.getNodeId())) {
          nodeLeavingLock.get().release();
        }
        if (nodeListener != null && active) {
          VertxInternal vertxInternal = (VertxInternal) appContext.getVertx();
          vertxInternal.getOrCreateContext().executeBlocking(runOnWorkingPool -> {
            nodeListener.nodeLeft(receivedNodeId);
            runOnWorkingPool.complete();
          }, taskQueue, res -> {
            if (log.isTraceEnabled()) {
              log.trace("[" + appContext.getNodeId() + "]" + " Node: " + receivedNodeId + " has been removed from nodeListener.");
            }
          });
        }
        break;
      }
    }
  }

  /**
   * Creates simple tcp server used to receive heart beat messages from consul agent and acknowledge them.
   */
  private Future<Void> createTcpServer() {
    Promise<Void> promise = Promise.promise();
    /*
     * Figuring out the node's host address (host ip address) is always tricky especially in case
     * multiple network interfaces present on the host this cluster manager operates on.
     * @See https://github.com/romalev/vertx-consul-cluster-manager/issues/92.
     */
    String hostAddress = consulClusterManagerConfig.getString("nodeHost");
    if (Objects.nonNull(hostAddress) && !hostAddress.isEmpty()) {
      nodeTcpAddress.put("host", hostAddress);
    } else {
      try {
        nodeTcpAddress.put("host", InetAddress.getLocalHost().getHostAddress());
      } catch (UnknownHostException e) {
        log.error(e);
        promise.fail(e);
      }
    }

    tcpServer = appContext.getVertx().createNetServer(new NetServerOptions(nodeTcpAddress));
    tcpServer.connectHandler(event -> {
    }); // node's tcp server acknowledges consul's heartbeat message.
    tcpServer.listen(listenEvent -> {
      if (listenEvent.succeeded()) {
        nodeTcpAddress.put("port", listenEvent.result().actualPort());
        promise.complete();
      } else promise.fail(listenEvent.cause());
    });
    return promise.future();
  }

  /**
   * Shut downs CM tcp server.
   */
  private Future<Void> shutdownTcpServer() {
    Promise<Void> promise = Promise.promise();
    if (tcpServer != null) tcpServer.close(promise);
    else promise.complete();
    return promise.future();
  }

  /**
   * Registers central @{code SERVICE_NAME} service that is dedicated to vert.x cluster management.
   *
   * @return {@link Future} holding the result.
   */
  private Future<Void> registerService() {
    Promise<Void> promise = Promise.promise();
    ServiceOptions serviceOptions = new ServiceOptions();
    serviceOptions.setName(SERVICE_NAME);
    serviceOptions.setTags(Collections.singletonList("vertx-clustering"));
    serviceOptions.setId(SERVICE_NAME);

    appContext.getConsulClient().registerService(serviceOptions, asyncResult -> {
      if (asyncResult.failed()) {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to register vert.x cluster management service.", asyncResult.cause());
        promise.fail(asyncResult.cause());
      } else promise.complete();
    });
    return promise.future();
  }

  /**
   * Register tcp check (dedicated to vert.x node) in consul.
   * Gets the node's tcp check registered within consul.
   * These checks make an TCP connection attempt every Interval (in our case this {@code TCP_CHECK_INTERVAL}) to the specified IP/hostname and port.
   * The status of the service depends on whether the connection attempt is successful (ie - the port is currently accepting connections).
   * If the connection is accepted, the status is success, otherwise the status is critical.
   * In the case of a hostname that resolves to both IPv4 and IPv6 addresses, an attempt will be made to both addresses,
   * and the first successful connection attempt will result in a successful check.
   *
   * @return {@link Future} holding the result.
   */
  private Future<Void> registerTcpCheck() {
    Promise<Void> promise = Promise.promise();
    CheckOptions checkOptions = new CheckOptions()
      .setName(checkId)
      .setNotes("This check is dedicated to service with id: " + appContext.getNodeId())
      .setId(checkId)
      .setTcp(nodeTcpAddress.getString("host") + ":" + nodeTcpAddress.getInteger("port"))
      .setServiceId(SERVICE_NAME)
      .setInterval(TimeUnit.MILLISECONDS.toSeconds(TCP_CHECK_INTERVAL) + "s")
      .setStatus(CheckStatus.PASSING);
    appContext.getConsulClient().registerCheck(checkOptions, result -> {
      if (result.failed()) {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to register check: " + checkOptions.getId(), result.cause());
        promise.fail(result.cause());
      } else promise.complete();
    });
    return promise.future();
  }

  /**
   * Deregisters vert.x node's tcp check.
   *
   * @return {@link Future} holding the result.
   */
  private Future<Void> deregisterTcpCheck() {
    Promise<Void> promise = Promise.promise();
    appContext.getConsulClient().deregisterCheck(checkId, resultHandler -> {
      if (resultHandler.succeeded()) promise.complete();
      else {
        log.error("[" + appContext.getNodeId() + "]" + " - Failed to deregister check: " + checkId, resultHandler.cause());
        promise.fail(resultHandler.cause());
      }
    });
    return promise.future();
  }

  /**
   * Tries to deregister all failing tcp checks from {@code SERVICE_NAME}.
   */
  private Future<Void> deregisterFailingTcpChecks() {
    Promise<CheckList> checkListPromise = Promise.promise();
    appContext.getConsulClient().healthChecks(SERVICE_NAME, checkListPromise);
    return checkListPromise.future().compose(checkList -> {
      List<Future> futures = new ArrayList<>();
      checkList.getList().forEach(check -> {
        if (check.getStatus() == CheckStatus.CRITICAL) {
          Promise<Void> promise = Promise.promise();
          appContext.getConsulClient().deregisterCheck(check.getId(), promise);
          futures.add(promise.future());
        }
      });
      return CompositeFuture.all(futures).compose(compositeFuture -> Future.succeededFuture());
    });
  }
}
