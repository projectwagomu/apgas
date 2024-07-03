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

import apgas.Configuration;
import apgas.Constructs;
import apgas.Place;
import apgas.impl.GlobalRuntimeImpl;
import apgas.util.ConsolePrinter;
import java.util.ArrayList;
import java.util.List;

/**
 * Class in charge of implementing the communication protocol with the scheduler on the APGAS
 * runtime. This class serves as the recipient for malleable or evolving requests coming from the
 * scheduler and the sender for evolving requests coming from running programs.
 *
 * @author Patrick Finnerty
 */
public abstract class ElasticCommunicator {

  /**
   * Used to make sure both pre(Grow/Shrink) and post(Grow/Shrink) have been finished before the
   * ElasticCommunicator is disabled
   */
  public final Object lock = new Object();

  /**
   * Double locks shrinking growin in addition to lock as evolvingShrink is called "externally" from
   * EvolvingMonitor
   */
  public final Object paranoidDoubleLock = new Object();

  private final String elasticityMode = Configuration.CONFIG_APGAS_ELASTIC.get();

  /**
   * Informs the scheduler that the hosts given as argument was released
   *
   * @param hosts released following a shrink order
   */
  protected abstract void hostReleased(List<String> hosts);

  /**
   * Method to call by the extending class when a grow order is received from the scheduler.
   *
   * @param nbPlacesToGrow number of places to grow by
   * @param hosts hosts to use to spawn new places
   */
  protected final void elasticGrow(int nbPlacesToGrow, List<String> hosts) {
    synchronized (paranoidDoubleLock) {
      ConsolePrinter.getInstance().printlnAlways("elasticGrow() entered.");

      // Perform the user-defined pre-grow tasks
      final GlobalRuntimeImpl impl = GlobalRuntimeImpl.getRuntime();
      if (elasticityMode.equals(Configuration.APGAS_ELASTIC_EVOLVING)) {
        ConsolePrinter.getInstance().printlnAlways("Evolving Pre Grow");
        impl.evolvingHandler.preGrow(nbPlacesToGrow);
      } else {
        ConsolePrinter.getInstance().printlnAlways("Malleable Pre Grow");
        impl.malleableHandler.preGrow(nbPlacesToGrow);
      }
      final List<? extends Place> oldPlaces = Constructs.places();

      // Grow
      impl.startMallPlacesBlocking(nbPlacesToGrow, hosts);

      // Check what the new places are
      final List<? extends Place> nowPlaces = Constructs.places();
      final ArrayList<Place> newPlaces = new ArrayList<>();
      for (final Place p : nowPlaces) {
        if (!oldPlaces.contains(p)) {
          newPlaces.add(p);
        }
      }

      // Inform the running program of the end of this grow operation
      if (elasticityMode.equals(Configuration.APGAS_ELASTIC_EVOLVING)) {
        ConsolePrinter.getInstance().printlnAlways("Evolving Post Grow");
        GlobalRuntimeImpl.getRuntime()
            .evolvingHandler
            .postGrow(nowPlaces.size(), oldPlaces, newPlaces);
      } else {
        ConsolePrinter.getInstance().printlnAlways("Malleable Post Grow");
        GlobalRuntimeImpl.getRuntime()
            .malleableHandler
            .postGrow(nowPlaces.size(), oldPlaces, newPlaces);
      }

      // Start cpuLoadEvaluation on new places if in evolving mode
      if (elasticityMode.equals(Configuration.APGAS_ELASTIC_EVOLVING)) {
        GetLoad _load = GlobalRuntimeImpl.getRuntime().EVOLVING.load;
        for (Place place : newPlaces) {
          ConsolePrinter.getInstance()
              .printlnAlways("Start obtaining PlaceLoad on place " + place.id);
          GlobalRuntimeImpl.getRuntime()
              .immediateAsyncAt(
                  place,
                  () -> {
                    GlobalRuntimeImpl.getRuntime().EVOLVING = new EvolvingMonitor();
                    GlobalRuntimeImpl.getRuntime().EVOLVING.startObtainPlaceLoad(_load);
                  });
        }
      }
    }
  }

  /**
   * Method to call by the extending class when a shrink order is received from the scheduler.
   *
   * @param nbPlacesToFree number of places to release
   */
  @SuppressWarnings("unchecked")
  protected final void malleableShrink(int nbPlacesToFree) {
    synchronized (paranoidDoubleLock) {
      ConsolePrinter.getInstance().printlnAlways("malleableShrink() entered.");

      // Perform the user-defined pre-shrink tasks
      final List<Place> toRelease =
          GlobalRuntimeImpl.getRuntime().malleableHandler.preShrink(nbPlacesToFree);

      // Obtain the hostnames of the places to release and shutdown these places
      final List<String> hosts =
          GlobalRuntimeImpl.getRuntime().shutdownMallPlacesBlocking(toRelease);

      // Inform the scheduler of the released hosts
      hostReleased(hosts);

      // Inform the running program of the end of the operation
      final List<Place> places = (List<Place>) Constructs.places();
      GlobalRuntimeImpl.getRuntime().malleableHandler.postShrink(places.size(), toRelease);
    }
  }

  @SuppressWarnings("unchecked")
  protected final void evolvingShrink(ArrayList<Place> placeToShrink) {
    synchronized (paranoidDoubleLock) {
      ConsolePrinter.getInstance().printlnAlways("evolvingShrink() entered.");
      // Perform the user-defined pre - shrink tasks
      final List<Place> toRelease =
          GlobalRuntimeImpl.getRuntime().evolvingHandler.preShrink(placeToShrink);
      // Obtain the hostnames of the places to release and shutdown these places
      final List<String> hosts =
          GlobalRuntimeImpl.getRuntime().shutdownMallPlacesBlocking(toRelease);

      // Inform the scheduler of the released hosts
      for (String h : hosts) {
        String msg = "Evolving;Release;" + h;
        sendToScheduler(msg);
        ConsolePrinter.getInstance().printlnAlways("sendtoScheduler : " + msg);
      }

      // Inform the running program of the end of the operation
      final List<Place> places = (List<Place>) Constructs.places();
      GlobalRuntimeImpl.getRuntime().evolvingHandler.postShrink(places.size(), toRelease);
    }
  }

  /**
   * Method called when the running program moves into a state where it is capable of changing its
   * number of running hosts and as a result may receive orders from the scheduler.
   *
   * @throws Exception if thrown by the underlying implementation
   */
  public abstract void start() throws Exception;

  /**
   * Method called when the running program is about to terminate. This method gives the opportunity
   * to the communicator to shut down cleanly.
   */
  public abstract void stop();

  /**
   * Sends a message to the external scheduler
   *
   * @param message
   */
  public abstract void sendToScheduler(final String message);

  public abstract void interrupt();
}
