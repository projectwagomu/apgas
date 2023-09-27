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
import java.util.Collection;

/** The {@link PlaceLocalIntArray} class implements a map from places to {@code int} arrays. */
public class PlaceLocalIntArray extends PlaceLocalObject {

  /** The local array. */
  private final int[] array;

  /**
   * Initializes the local array.
   *
   * @param n the length of the local array
   */
  private PlaceLocalIntArray(int n) {
    array = new int[n];
  }

  /**
   * Constructs a {@link PlaceLocalArray} instance.
   *
   * @param places a collection of places with no repetition
   * @param localLength the length of each chunk
   * @return the place local array
   */
  public static PlaceLocalIntArray make(Collection<? extends Place> places, int localLength) {
    return PlaceLocalObject.make(places, () -> new PlaceLocalIntArray(localLength));
  }

  /**
   * Returns the local array element at the specified index
   *
   * @param index an index into the local array
   * @return the array element
   */
  public int get(int index) {
    return array[index];
  }

  /**
   * Sets the local array element at the specified index
   *
   * @param index an index into the local array
   * @param t the desired value
   */
  public void set(int index, int t) {
    array[index] = t;
  }
}
