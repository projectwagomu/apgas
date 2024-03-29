/*
 * Copyright (c) 2023 Wagomu project.
 *
 * This program and the accompanying materials are made available to you under
 * the terms of the Eclipse Public License 1.0 which accompanies this
 * distribution,
 * and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 */

package apgas.util;

import static apgas.Constructs.async;
import static apgas.Constructs.asyncAt;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;

import apgas.DeadPlaceException;
import apgas.Place;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@link GlobalID} class provides globally unique IDs and mechanisms to attach place-specific
 * data to these IDs.
 */
public class GlobalID implements Serializable {

  /** Internal counter. */
  private static final AtomicInteger count = new AtomicInteger();

  /** Internal map. */
  private static final Map<GlobalID, Object> map = new ConcurrentHashMap<>();

  private static final Object NULL = new Object();

  private static final long serialVersionUID = 5480936903198352190L;

  /** The {@link Place} where this {@link GlobalID} was instantiated. */
  public final Place home;

  /**
   * The local ID component of this {@link GlobalID} instance.
   *
   * <p>This local ID is guaranteed to be unique for this place but not across all places.
   */
  private final int lid;

  /** Constructs a new {@link GlobalID}. */
  public GlobalID() {
    home = here();
    lid = count.getAndIncrement();
  }

  @Override
  public boolean equals(Object that) {
    return that instanceof GlobalID && gid() == ((GlobalID) that).gid();
  }

  /**
   * Returns the value associated with this {@link GlobalID} instance.
   *
   * @return the current value
   */
  public Object getHere() {
    final Object result = map.get(this);
    return result == NULL ? null : result;
  }

  /**
   * Returns the value associated with this {@link GlobalID} instance if any, or {@code
   * defaultValue} if none.
   *
   * @param defaultValue the default value
   * @return the current or default value
   */
  public Object getOrDefaultHere(Object defaultValue) {
    final Object result = map.getOrDefault(this, defaultValue);
    return result == NULL ? null : result;
  }

  /**
   * The globally unique {@code long} ID of this {@link GlobalID} instance.
   *
   * @return a globally unique ID
   */
  public long gid() {
    return ((long) home.id << 32) + lid;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(gid());
  }

  /**
   * Associates the given value with this {@link GlobalID} instance.
   *
   * @param value the value to associate with this global ID
   * @return the previous value
   */
  public Object putHere(Object value) {
    final Object result = map.put(this, value == null ? NULL : value);
    return result == NULL ? null : result;
  }

  /**
   * If this {@link GlobalID} instance is not already associated with a value associates it with the
   * given value.
   *
   * @param value the value to associate with this global ID
   * @return the previous value
   */
  public Object putHereIfAbsent(Object value) {
    final Object result = map.putIfAbsent(this, value == null ? NULL : value);
    return result == NULL ? null : result;
  }

  /**
   * Removes the value associated with this {@link GlobalID} instance in a collection of places.
   *
   * <p>Masks {@link DeadPlaceException} instances if any.
   *
   * @param places where to remove the value
   */
  public void remove(Collection<? extends Place> places) {
    final GlobalID that = this;
    finish(
        () -> {
          for (final Place p : places) {
            try {
              asyncAt(
                  p,
                  () -> {
                    that.removeHere();
                  });
            } catch (final DeadPlaceException e) {
              async(
                  () -> {
                    throw e;
                  });
            }
          }
        });
  }

  /**
   * Removes the value associated with this {@link GlobalID} instance if any.
   *
   * @return the removed value
   */
  public Object removeHere() {
    final Object result = map.remove(this);
    return result == NULL ? null : result;
  }

  @Override
  public String toString() {
    return "gid(" + gid() + ")";
  }
}
