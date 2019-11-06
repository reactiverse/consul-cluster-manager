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

import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.spi.cluster.ChoosableIterable;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ChoosableSet<T> implements ChoosableIterable<T>, Serializable {

  private final Set<T> ids;
  private volatile Iterator<T> iter;

  private ChoosableSet(int initialSize) {
    ids = new ConcurrentHashSet<>(initialSize);
  }

  private static ChoosableSet instance;
  static synchronized <T> ChoosableSet<T> getInstance(int initialSize) {
    if (instance == null) {
      instance = new ChoosableSet(initialSize);
    }
    return instance;
  }

  public Set<T> getIds() {
    return ids;
  }

  public int size() {
    return ids.size();
  }

  void add(T elem) {
    ids.add(elem);
  }

  boolean remove(T elem) {
    return ids.remove(elem);
  }

  public void merge(Set<T> toMerge) {
    ids.addAll(toMerge);
  }

  public boolean isEmpty() {
    return ids.isEmpty();
  }

  public boolean contains(T elem) {
    return ids.contains(elem);
  }

  @Override
  public Iterator<T> iterator() {
    return ids.iterator();
  }

  public synchronized T choose() {
    if (!ids.isEmpty()) {
      if (iter == null || !iter.hasNext()) {
        iter = ids.iterator();
      }
      try {
        return iter.next();
      } catch (NoSuchElementException e) {
        return null;
      }
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    StringBuilder string = new StringBuilder();
    ids.forEach(t -> string.append(t.toString()).append(";"));
    return string.toString();
  }
}

