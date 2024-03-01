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

import static apgas.Constructs.*;

import apgas.Place;
import apgas.impl.GlobalRuntimeImpl;
import apgas.util.ConsolePrinter;
import java.util.ArrayList;
import java.util.List;

/**
 * Simplistic program illustrating how to implement an evolving program
 *
 * @author Patrick Finnerty
 */
public class EvolvingDummyApplication {

  /**
   * Evolving example main which performs no computation and just waits till the specified time (in
   * seconds) elapses
   *
   * @param args time to elapse in seconds
   */
  public static void main(String[] args) {
    final int toElapse = Integer.parseInt(String.valueOf(60));
    finish(
        () -> {
          for (final Place p : places()) {
            asyncAt(
                p,
                () -> {
                  System.out.println(p + " is running.");
                });
          }
        });

    /* Enable evolving by defining the handler
     * Set to evolving variant by choosing a GetLoad() variant
     * Cpu-based evaluation: GetCpuLoad
     * Task-based evaluation: GetTaskLoad
     */
    defineEvolvingHandler(new EvolvingHandler(), new GetCpuLoad());

    finish(
        () -> {
          for (final Place p : places()) {
            asyncAt(
                p,
                () -> {
                  System.out.println(p + " is running.");
                  // Dirty fixing "tasks could be lost when shrinking"
                  final int _myid = here().id;
                  asyncAt(
                      place(0),
                      () -> {
                        GlobalRuntimeImpl.getRuntime()
                            .EVOLVING
                            .placeActiveRef
                            .get()
                            .put(_myid, true);
                      });
                });
          }
        });

    // Start dummy evolving computation for the indicated time
    final long startWait = System.nanoTime();
    while (System.nanoTime() - startWait < 1e9 * toElapse) {
      try {
        Thread.sleep(100); // Wait in 100ms increments
      } catch (final InterruptedException e) {
        // Do nothing in case of exception
      }
    }
    disableElasticCommunicator();
    System.out.println(toElapse + "s have elapsed, quiting");
  }

  static class EvolvingHandler implements apgas.impl.elastic.EvolvingHandler {

    private static final long serialVersionUID = 4003743679074722952L;

    @Override
    public void preGrow(int nbPlaces) {
      ConsolePrinter.getInstance()
          .printlnAlways("Handler received notification that places will increase by " + nbPlaces);
    }

    @Override
    public List<Place> preShrink(ArrayList<Place> placeToShrink) {
      System.out.println("Handler received request to release places " + placeToShrink);
      // Here the Application could add stuff to do before shrinking a Place.
      // This is empty in this test Application.
      return placeToShrink;
    }

    @Override
    public void postGrow(
        int nbPlaces, List<? extends Place> continuedPlaces, List<? extends Place> newPlaces) {
      ConsolePrinter.getInstance()
          .printlnAlways("Handler received notification that the grow operation is now complete");
      System.out.print("Continued places: [");
      for (final Place p : continuedPlaces) {
        System.out.print(p + " ");
      }
      System.out.println("]");

      System.out.print("New places: [");
      for (final Place p : newPlaces) {
        System.out.print(p + " ");
      }
      System.out.println("]");
    }

    @Override
    public void postShrink(int nbPlaces, List<? extends Place> removedPlaces) {
      ConsolePrinter.getInstance()
          .printlnAlways("Handler received notification that the shrink operation is now complete");
      System.out.print("Removed places: [");
      for (final Place p : removedPlaces) {
        System.out.print(p + " ");
      }
      System.out.println("]");
    }
  }
}
