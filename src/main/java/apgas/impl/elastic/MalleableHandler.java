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
package apgas.impl.elastic;

import apgas.Constructs;
import apgas.Place;
import java.io.Serializable;
import java.util.List;

/**
 * Interface which defines what to do when a malleable (shrink or grow) order comes from the
 * scheduler. The four method presented here are used to define what needs to be done before and
 * after the running program either increases or decreases its number of running processes. As this
 * depends on the program, this interface was designed to be generic enough to allow for any
 * implementation.
 *
 * <p>Note that all the methods defined in this interface will be run from Place0. If alterations to
 * the running program need to be performed on other places, the usual finish/asyncAt constructs
 * provided by class {@link Constructs} can be used.
 *
 * <p>Programmers wishing to make their program malleable should implement this interface and define
 * the handler using method {@link Constructs#defineMalleableHandle(MalleableHandler)} as soon as
 * the program is ready to receive grow or shrink orders from the scheduler.
 *
 * @author Patrick Finnerty
 */
public interface MalleableHandler extends Serializable {

  /**
   * Method called after the necessary number of places were added to the running program. The
   * distributed runtime has completed its adjustments and the running program can now resume normal
   * execution.
   *
   * @param nbPlaces number of places currently running
   * @param continuedPlaces list containing the places already present prior to the grow operation
   * @param newPlaces list containing the places that were added to the runtime
   */
  void postGrow(
      int nbPlaces, List<? extends Place> continuedPlaces, List<? extends Place> newPlaces);

  /**
   * Method called after the necessary number of places were removed from the running program. The
   * distributed runtime has completed all its operations and the running program can now resume
   * normal execution.
   *
   * @param nbPlaces number of places currently running
   * @param removedPlaces list containing the places that were removed
   */
  void postShrink(int nbPlaces, List<? extends Place> removedPlaces);

  /**
   * Method called prior to an increase in the number of processes in the running program. If any
   * preparation is needed prior to the increase in the number of places is needed, they should be
   * performed before this method returns.
   *
   * <p>Method {@link #postGrow(int, List, List)} will be called next.
   *
   * @param nbPlaces number of places that will be added to the running program
   */
  void preGrow(int nbPlaces);

  /**
   * Method called when a shrink order is received by the scheduler. In this method, all the
   * preparations necessary to the program prior to the actual release of places should be performed
   * before returning a list containing the list of places to release. The places returned in the
   * list should be distinct and match the number of places indicated by the parameter given to the
   * method.
   *
   * <p>Method {@link #postShrink(int, List)} will be called next.
   *
   * @param nbPlaces number of places that have to be released
   * @return the places that will be released
   */
  List<Place> preShrink(int nbPlaces);
}
