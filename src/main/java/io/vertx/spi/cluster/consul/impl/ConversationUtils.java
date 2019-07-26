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

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.impl.ClusterSerializable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.util.Base64;
import java.util.Optional;

/**
 * Dedicated utility to marshal objects into strings and un-marshal strings into objects.
 * Consul client doesn't support writing byte arrays to consul KV store.
 * In order to address the ability to save different types of java objects we:
 * - serialize objects and get their byte arrays.
 * - base64 encode this byte arrays - as a result we receive {@link String}.
 * - use this base 64 encoded string as "value" to put into consul kv store.
 * <p>
 * Note: consider KRYO: [https://github.com/EsotericSoftware/kryo] to replace default java serialization here.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 * @See {@link ConsulEntry}
 */
class ConversationUtils {

  private static final String KEY_TAG = "KEY";
  private static final String VALUE_TAG = "VALUE";
  private static final String NODE_ID_TAG = "NODE_ID";
  private static final String TTL_TAG = "TTL";
  private static final String INFO_TAG = "INFO";

  /**
   * Serializes key and value and also appends the metadata.
   *
   * @param key    actual key of an entry.
   * @param value  action value of an entry.
   * @param nodeId id of the node that performs this operation.
   * @param ttl    optional ttl that might be placed on entry.
   * @param <K>    key type.
   * @param <V>    value type.
   * @return json object that gets shipped as value to consul kv store.
   * @throws IOException in case an entry can be properly serialized.
   */
  static <K, V> String asString(K key, V value, String nodeId, Optional<Long> ttl) throws IOException {
    JsonObject jsonObject = new JsonObject()
      .put(KEY_TAG, asString(key))
      .put(VALUE_TAG, asString(value))
      .put(NODE_ID_TAG, nodeId)
      .put(INFO_TAG, value.toString());
    ttl.ifPresent(ttlValue -> jsonObject.put(TTL_TAG, ttlValue));

    return jsonObject.encodePrettily();
  }

  /**
   * De-serializes consul map'value to an internal {@link ConsulEntry}
   *
   * @param consulValue base 64 encoded serialized consul value.
   * @param <K>         key type.
   * @param <V>         value type.
   * @return an internal {@link ConsulEntry}
   * @throws Exception in case consul value can be properly de-serialized.
   */
  static <K, V> ConsulEntry<K, V> asConsulEntry(String consulValue) throws Exception {
    JsonObject jsonObject = new JsonObject(consulValue);
    K key = asObject(jsonObject.getString(KEY_TAG));
    V value = asObject(jsonObject.getString(VALUE_TAG));
    String nodeId = jsonObject.getString(NODE_ID_TAG);
    Optional<Long> ttl = Optional.ofNullable(jsonObject.getLong(TTL_TAG));
    return new ConsulEntry<>(key, value, nodeId, ttl);
  }

  /**
   * Gets ttl out of consul value which is a {@link JsonObject}.
   *
   * @param consulEntry consul value.
   * @return ttl.
   */
  static Optional<Long> asTtlConsulEntry(String consulEntry) {
    JsonObject jsonObject = new JsonObject(consulEntry);
    Optional<Long> ttl = Optional.ofNullable(jsonObject.getLong(TTL_TAG));
    return ttl;
  }

  /**
   * {@link Future} wrapper around asString.
   */
  static <K, V> Future<String> asFutureString(K key, V value, String nodeId) {
    Promise<String> result = Promise.promise();
    try {
      result.complete(asString(key, value, nodeId, Optional.empty()));
    } catch (IOException e) {
      result.fail(e);
    }
    return result.future();
  }

  /**
   * {@link Future} wrapper around asString that takes into account TTL.
   */
  static <K, V> Future<String> asFutureString(K key, V value, String nodeId, Long ttl) {
    Promise<String> result = Promise.promise();
    try {
      result.complete(asString(key, value, nodeId, Optional.ofNullable(ttl)));
    } catch (IOException e) {
      result.fail(e);
    }
    return result.future();
  }

  static <K, V> Future<ConsulEntry<K, V>> asFutureConsulEntry(String object) {
    Promise<ConsulEntry<K, V>> result = Promise.promise();
    if (object == null) result.complete();
    else {
      try {
        result.complete(asConsulEntry(object));
      } catch (Exception e) {
        result.fail(e);
      }
    }
    return result.future();
  }

  /**
   * Serializes any type of {@link Object} into base 64 encoded {@link String} by taking into account {@link ClusterSerializable}.
   *
   * @param object to be serialized.
   * @return serialized, base 64 encoded object.
   * @throws IOException in case object can't get serialized.
   */
  private static String asString(Object object) throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(byteOut);
    if (object instanceof ClusterSerializable) {
      ClusterSerializable clusterSerializable = (ClusterSerializable) object;
      dataOutput.writeBoolean(true);
      dataOutput.writeUTF(object.getClass().getName());
      Buffer buffer = Buffer.buffer();
      clusterSerializable.writeToBuffer(buffer);
      byte[] bytes = buffer.getBytes();
      dataOutput.writeInt(bytes.length);
      dataOutput.write(bytes);
    } else {
      dataOutput.writeBoolean(false);
      ByteArrayOutputStream javaByteOut = new ByteArrayOutputStream();
      ObjectOutput objectOutput = new ObjectOutputStream(javaByteOut);
      objectOutput.writeObject(object);
      dataOutput.write(javaByteOut.toByteArray());
    }
    return Base64.getEncoder().encodeToString(byteOut.toByteArray());
  }


  /**
   * De-serializes a plain {@link String} into an actual object by taking into account {@link ClusterSerializable}.
   *
   * @param object base 63 encoded string object for de - serialization.
   * @param <T>    type of de-serialized object.
   * @return de-serialized object.
   * @throws Exception in case object can't get de-serialized.
   */
  private static <T> T asObject(String object) throws Exception {
    final byte[] data = Base64.getDecoder().decode(object);
    ByteArrayInputStream byteIn = new ByteArrayInputStream(data);
    DataInputStream in = new DataInputStream(byteIn);
    boolean isClusterSerializable = in.readBoolean();
    if (isClusterSerializable) {
      String className = in.readUTF();
      Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
      int length = in.readInt();
      byte[] body = new byte[length];
      in.readFully(body);
      try {
        ClusterSerializable clusterSerializable;
        //check clazz if have a public Constructor method.
        if (clazz.getConstructors().length == 0) {
          Constructor<T> constructor = (Constructor<T>) clazz.getDeclaredConstructor();
          constructor.setAccessible(true);
          clusterSerializable = (ClusterSerializable) constructor.newInstance();
        } else {
          clusterSerializable = (ClusterSerializable) clazz.newInstance();
        }
        clusterSerializable.readFromBuffer(0, Buffer.buffer(body));
        return (T) clusterSerializable;
      } catch (Exception e) {
        throw new IllegalStateException("Failed to load class " + e.getMessage(), e);
      }
    } else {
      byte[] body = new byte[in.available()];
      in.readFully(body);
      ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(body));
      return (T) objectIn.readObject();
    }
  }
}
