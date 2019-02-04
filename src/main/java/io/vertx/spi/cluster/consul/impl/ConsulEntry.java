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

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a simple entry of any of the cluster maps located within consul kv store.
 * <p>
 * Key and value of any of the cluster maps can ONLY AND ONLY be represented as simple strings. In order to support different type of keys or/and values (ie. integers, doubles, etc)
 * we serialize the key + value and the result gets shipped to consul as a value. We build cluster map out of consul kv store map by deserializing a value which essentially is an key and value.
 *
 * @param <K> actual type of key.
 * @param <V> actual type of value.
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 * @See {@link ConversationUtils}
 */
final class ConsulEntry<K, V> implements Serializable {
  final private K key;
  final private V value;
  final private String nodeId; // nodeId given consul entry belongs to
  final private Optional<Long> ttl; // ttl on entry if present.

  ConsulEntry(K key, V value, String nodeId, Optional<Long> ttl) {
    this.key = key;
    this.value = value;
    this.nodeId = nodeId;
    this.ttl = ttl;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public String getNodeId() {
    return nodeId;
  }

  public Optional<Long> getTtl() {
    return ttl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConsulEntry<?, ?> that = (ConsulEntry<?, ?>) o;
    return Objects.equals(getKey(), that.getKey()) &&
      Objects.equals(getValue(), that.getValue()) &&
      Objects.equals(nodeId, that.nodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKey(), getValue(), nodeId);
  }

  @Override
  public String toString() {
    return "ConsulEntry{" +
      "key=" + key +
      ", value=" + value +
      ", nodeId='" + nodeId + '\'' +
      '}';
  }
}
