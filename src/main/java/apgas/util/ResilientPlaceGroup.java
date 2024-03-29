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

import static apgas.Constructs.places;

import apgas.Place;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ResilientPlaceGroup implements Serializable {

  /** Serial Version UID */
  private static final long serialVersionUID = -4918341729994198948L;

  /** Array containing the Place objects for the current execution */
  private final Place[] array;

  /** Largest place number currently allocated */
  private int max;

  public ResilientPlaceGroup(int size) {
    array = new Place[size];
    int id = 0;
    for (final Place p : places()) {
      array[id] = p;
      id++;
      if (id == size) {
        max = p.id;
        return;
      }
    }
    System.err.println("[APGAS] Too few places to initialize the ResilientPlaceGroup. Aborting.");
    System.exit(1);
  }

  public boolean contains(Place place) {
    for (final Place element : array) {
      if (element.id == place.id) {
        return true;
      }
    }
    return false;
  }

  public void fix() {
    final List<? extends Place> places = places();
    final Iterator<? extends Place> it = places.iterator();
    Place spare;
    try {
      for (int id = 0; id < array.length; ++id) {
        if (!places.contains(array[id])) {
          for (; ; ) {
            spare = it.next();
            if (spare.id > max) {
              break;
            }
          }
          array[id] = spare;
          max = spare.id;
        }
      }
    } catch (final NoSuchElementException e) {
      System.err.println("[APGAS] Too few places to fix the ResilientPlaceGroup. Aborting.");
      System.exit(1);
    }
  }

  public Place get(int id) {
    return array[id];
  }

  public int indexOf(Place place) {
    for (int id = 0; id < array.length; ++id) {
      if (array[id].id == place.id) {
        return id;
      }
    }
    return -1;
  }

  public int size() {
    return array.length;
  }
}
