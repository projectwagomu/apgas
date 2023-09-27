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

import apgas.Place;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

/**
 * The {@link PlaceLocalArray} class implements a map from places to arrays.
 *
 * @param <T> the type of the array elements
 */
public class PlaceLocalArray<T extends Serializable> extends PlaceLocalObject {

  /** The local array. */
  private final T[] array;

  /**
   * Initializes the local array.
   *
   * @param array the local array
   */
  private PlaceLocalArray(T[] array) {
    this.array = array;
  }

  /**
   * Constructs a {@link PlaceLocalArray} instance.
   *
   * @param <T> the type of the array elements
   * @param places a collection of places with no repetition
   * @param localLength the length of each chunk
   * @param array an array of {@code T}
   * @return the place local array
   */
  @SafeVarargs
  public static <T extends Serializable> PlaceLocalArray<T> make(
      Collection<? extends Place> places, int localLength, T... array) {
    return PlaceLocalObject.make(
        places, () -> new PlaceLocalArray<>(Arrays.copyOf(array, localLength)));
  }

  /**
   * Returns the local array element at the specified index
   *
   * @param index an index into the local array
   * @return the array element
   */
  public T get(int index) {
    return array[index];
  }

  /**
   * Sets the local array element at the specified index
   *
   * @param index an index into the local array
   * @param t the desired value
   */
  public void set(int index, T t) {
    array[index] = t;
  }
}
