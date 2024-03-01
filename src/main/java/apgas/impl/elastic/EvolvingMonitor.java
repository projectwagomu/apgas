package apgas.impl.elastic;

import static apgas.Constructs.*;

import apgas.Configuration;
import apgas.Place;
import apgas.impl.GlobalRuntimeImpl;
import apgas.impl.HostManager;
import apgas.util.ConsolePrinter;
import apgas.util.GlobalRef;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Class in charge of changing APGAS Places dynamically at runtime.
 *
 * @author Ashatar
 */
public class EvolvingMonitor implements Serializable {
  /** Serial Version UID */
  private static final long serialVersionUID = -6696307096425437557L;

  /**
   * Global Ref on Hashmap for place active collection on place 0
   *
   * <p>Integer for place id, Boolean for active
   */
  public final GlobalRef<HashMap<Integer, Boolean>> placeActiveRef;

  /** Setting of hyper-threading from {@link Configuration} */
  private final boolean hyperthreading = Configuration.CONFIG_ELASTIC_HYPER.get();

  /** List of places containing the places with the lowest load to be shrunk excluding place 0 */
  private final ArrayList<Place> placeToShrink = new ArrayList<>();

  /**
   * Maximum time in milliseconds to wait for place changes. If this timer is exceeded, the waitLock
   * will be reset to false.
   */
  private final long timeoutWaitForPlaceChanges;

  /**
   * Load evaluation interval on place 0. It defines the time to wait between load evaluation cycles
   * (including possible shrink or grow requests).
   */
  private final int loadEvaluationInterval;

  /** Time between load checks on each place */
  private final int loadCheckInterval;

  /** Initial standby in milliseconds. Load evaluation on place 0 starts after this timer ends. */
  private final int initialStandby;

  /**
   * Load variable to get the instance of GetLoad class. The type of load to gather is set by
   * defining the Handler in {@link apgas.Constructs} called in the Program or in GLBs GLBcomputer.
   */
  public GetLoad load;

  /**
   * Place active is set to false on a specific place when the place is shut down. Stops load
   * evaluation on the place. When place 0 is shut down, load evaluation will be stopped as well.
   */
  public volatile boolean placeActive = true;

  /**
   * Global Ref on Hashmap for load collection on place 0
   *
   * <p>Integer for place id, Double for load value
   */
  GlobalRef<HashMap<Integer, Double>> loadMapRef;

  /** ID of the place to shrink Is negative in the beginning so that it does not match a place. */
  private int placeIdToShrink = -1;

  /** Load of the place with the lowest load. */
  private double min;

  private transient Thread obtainPlaceLoadThread;

  /** Load thresholds for low load from {@link Configuration} */
  private double lowLoadThreshold = Configuration.CONFIG_ELASTIC_LOWLOAD.get();

  /** Load thresholds for high load from {@link Configuration} */
  private double highLoadThreshold = Configuration.CONFIG_ELASTIC_HIGHLOAD.get();

  /** Load sum of all places combined */
  private double loadSum;

  /** Average load of all places combined */
  private double avgLoadAllPlaces;

  /** Load per place */
  private double loadPlace;

  /**
   * Lock for new place requests - default false. When waiting for place changes don't check if
   * growth/shrink is needed and don't get new load values until place changes are complete or
   * timeout set in timeoutWaitingForPlace is reached.
   */
  private Boolean waitingForPlaceChanges = false;

  /**
   * Set to current unixTime on sending a place change request.
   *
   * <p>Used for unlocking waitLock if place changes take very long.
   */
  private long startedWaiting = 0;

  /** UnixTime at this moment needed to compare to waitTimeout */
  private long now = 0;

  public EvolvingMonitor() {
    loadCheckInterval = 1000;
    loadEvaluationInterval = 1000;
    timeoutWaitForPlaceChanges = 10000;
    initialStandby = 3000; //
    // Set thresholds to halve its value if hyper-threading is set to true
    if (hyperthreading) {
      highLoadThreshold = highLoadThreshold / 2;
      lowLoadThreshold = lowLoadThreshold / 2;
    }
    this.loadMapRef = new GlobalRef<>(new HashMap<>());
    this.placeActiveRef = new GlobalRef<>(new HashMap<>());
  }

  /**
   * Method to start the collection of loads and evaluation of said loads. Calls {@code
   * obtainPlaceLoad} an every place and {@code loadEvaluation} on place 0.
   */
  public void evolve(GetLoad load) {
    if (this.load == null) {
      this.load = load;
    }
    // start obtaining the load on all places
    for (final Place place : places()) {
      GlobalRuntimeImpl.getRuntime()
          .immediateAsyncAt(
              place,
              () -> {
                GlobalRuntimeImpl.getRuntime().EVOLVING.startObtainPlaceLoad(load);
              });
    }
    ConsolePrinter.getInstance()
        .printlnAlways(
            "[Evolving] Thresholds: high load above "
                + highLoadThreshold
                + " %"
                + " low load below "
                + lowLoadThreshold
                + " %");
    loadEvaluation();
  }

  /**
   * Method starts uncounted immediate async thread on place_0 to evaluate load of places. It waits
   * the initialStandby time before repeating evaluation and sleeping. The evaluation part copies
   * the loadMap from the GlobalRef into a temporary HashMap called currentLoadMap to avoid changes
   * while evaluating.
   */
  private void loadEvaluation() {
    final GlobalRuntimeImpl impl = GlobalRuntimeImpl.getRuntime();
    async(
        () -> {
          try {
            Thread.sleep(initialStandby);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          while (placeActive) {
            try {
              Thread.sleep(loadEvaluationInterval);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            if (!this.placeActiveRef.get().containsValue(Boolean.TRUE)
                && GlobalRuntimeImpl.getRuntime().pool.getRunningThreadCount() == 1) {
              ConsolePrinter.getInstance().printlnAlways("[Evolving] loadEvaluation async return");
              return;
            }

            if (!GlobalRuntimeImpl.getRuntime().EVOLVING.waitingForPlaceChanges) {
              calcMinAndAvgLoad();
              /*ConsolePrinter.getInstance()
              .printlnAlways(
                      "[Evolving] shrinkBlockingRef: " + shrinkBlockingRef.get() + ".");*/
              // Growing
              if (avgLoadAllPlaces > highLoadThreshold) {
                List<String> hosts = new ArrayList<>();
                if (impl.hostManager
                    .getHostNames()
                    .get(0)
                    .contains("its-cs")) { // for kassel cluster

                  HostManager.Host nextHost = impl.hostManager.getNextHost();
                  if (nextHost != null) {
                    startedWaiting = System.nanoTime();
                    ConsolePrinter.getInstance()
                        .printlnAlways(
                            "[Evolving] Action Grow started "
                                + (startedWaiting - impl.startupTime) / 1e9
                                + " seconds after program start. places()="
                                + places()
                                + ", numPlaces="
                                + places().size()
                                + ", newPlaceID="
                                + impl.hostManager.peekNewPlaceId());

                    ConsolePrinter.getInstance()
                        .printlnAlways("[Evolving] Next Host Cluster: " + nextHost);
                    hosts.add(nextHost.getHostName());
                    GlobalRuntimeImpl.getRuntime().EVOLVING.waitingForPlaceChanges = true;
                    GlobalRuntimeImpl.getRuntime().elasticCommunicator.elasticGrow(1, hosts);
                    long now = System.nanoTime();
                    ConsolePrinter.getInstance()
                        .printlnAlways(
                            "[Evolving] Action Grow finished "
                                + (now - impl.startupTime) / 1e9
                                + " seconds after program start. Took "
                                + (now - startedWaiting) / 1e9
                                + " seconds. places()="
                                + places()
                                + ", numPlaces="
                                + places().size()
                                + ", newPlaceID="
                                + places().get(places().size() - 1).id);
                  } else {
                    ConsolePrinter.getInstance()
                        .printlnAlways(
                            "[Evolving] All hosts have max attached places - no free hosts available.");
                    continue;
                  }
                  hosts.clear();
                } else if (impl.hostManager.getNextHost() != null) { // for local testing
                  startedWaiting = System.nanoTime();
                  ConsolePrinter.getInstance()
                      .printlnAlways(
                          "[Evolving] Action Grow started "
                              + (startedWaiting - impl.startupTime) / 1e9
                              + " seconds after program start. places()="
                              + places()
                              + ", numPlaces="
                              + places().size()
                              + ", newPlaceID="
                              + impl.hostManager.peekNewPlaceId());

                  hosts.add(impl.hostManager.getHostNames().get(0));
                  GlobalRuntimeImpl.getRuntime().EVOLVING.waitingForPlaceChanges = true;
                  GlobalRuntimeImpl.getRuntime().elasticCommunicator.elasticGrow(1, hosts);
                  long now = System.nanoTime();
                  ConsolePrinter.getInstance()
                      .printlnAlways(
                          "[Evolving] Action Grow finished "
                              + (now - impl.startupTime) / 1e9
                              + " seconds after program start. Took "
                              + (now - startedWaiting) / 1e9
                              + " seconds. places()="
                              + places()
                              + ", numPlaces="
                              + places().size()
                              + ", newPlaceID="
                              + places().get(places().size() - 1).id);
                } else { // Reminder: other clusters do not work currently
                  ConsolePrinter.getInstance()
                      .printlnAlways("[Evolving] No Host for growing available.");
                  continue;
                }

                // Shrinking
              } else if ((min == 0 || avgLoadAllPlaces < lowLoadThreshold) && places().size() > 1) {
                GlobalRuntimeImpl.getRuntime().EVOLVING.waitingForPlaceChanges = true;
                startedWaiting = System.nanoTime();
                ConsolePrinter.getInstance()
                    .printlnAlways(
                        "[Evolving] Action Shrink started "
                            + (startedWaiting - impl.startupTime) / 1e9
                            + " seconds after program start. places()="
                            + places()
                            + ", numPlaces="
                            + places().size()
                            + ", shrinkPlaceID="
                            + placeIdToShrink);

                placeToShrink.add(GlobalRuntimeImpl.getRuntime().place(placeIdToShrink));

                if (placeActive) {
                  GlobalRef<CountDownLatch> waitPlaceActive =
                      new GlobalRef<>(new CountDownLatch(1));
                  GlobalRuntimeImpl.getRuntime()
                      .immediateAsyncAt(
                          place(placeIdToShrink),
                          () -> {
                            GlobalRuntimeImpl.getRuntime().EVOLVING.stopObtainPlaceLoad();
                            GlobalRuntimeImpl.getRuntime()
                                .immediateAsyncAt(
                                    place(0),
                                    () -> {
                                      waitPlaceActive.get().countDown();
                                    });
                          });
                  try {
                    waitPlaceActive.get().await();
                  } catch (final InterruptedException e) {
                    e.printStackTrace();
                  }
                }
                GlobalRuntimeImpl.getRuntime().elasticCommunicator.evolvingShrink(placeToShrink);
                long now = System.nanoTime();
                ConsolePrinter.getInstance()
                    .printlnAlways(
                        "[Evolving] Action Shrink finished "
                            + (now - impl.startupTime) / 1e9
                            + " seconds after program start. Took "
                            + (now - startedWaiting) / 1e9
                            + " seconds. places()="
                            + places()
                            + ", numPlaces="
                            + places().size()
                            + ", shrinkPlaceID="
                            + placeIdToShrink);
                placeToShrink.clear();
              }
              GlobalRuntimeImpl.getRuntime().EVOLVING.waitingForPlaceChanges = false;
            }
          }
        });
  }

  /**
   * Calculate the average load of all places. Gets Place with the lowest load and sets place id in
   * placeIdToShrink
   */
  private void calcMinAndAvgLoad() {
    min = 100;
    loadSum = 0.0;
    avgLoadAllPlaces = 0.0;
    // take snapshot of current hashmap status for evaluation
    HashMap<Integer, Double> currentLoadMap = loadMapRef.get();
    for (Map.Entry<Integer, Double> entry : currentLoadMap.entrySet()) {
      loadSum = loadSum + entry.getValue();
      if (entry.getKey() == 0) {
        continue;
      }
      if (entry.getValue() < min) {
        placeIdToShrink = entry.getKey();
        min = entry.getValue();
      }
    }
    avgLoadAllPlaces = loadSum / currentLoadMap.size();
    /* DecimalFormat df = new DecimalFormat("#.0");
    ConsolePrinter.getInstance()
    .printlnAlways(
            "[Evolving] Total Load: "
                    + df.format(avgLoadAllPlaces)
                    + " Load per Place: "
                    + currentLoadMap);*/
    currentLoadMap.clear();
  }

  void startObtainPlaceLoad(GetLoad load) {
    this.load = load;
    this.obtainPlaceLoadThread = new Thread(this::obtainPlaceLoad);
    this.obtainPlaceLoadThread.start();
  }

  void stopObtainPlaceLoad() {
    this.placeActive = false;
    while (this.obtainPlaceLoadThread.isAlive()) {
      try {
        TimeUnit.MILLISECONDS.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * The Method gets started on every place via startObtainPlaceLoad on all places to obtain a load.
   * With default values obtaining load takes ~1 second for cpu load and below 1 second for task
   * load then wait 1 second and repeat. The evaluation takes place as long as the place is active
   * and only happens if no shrink or grow action is in process. The evaluation will we stopped via
   * stopObtainPlaceLoad method when a place gets shut down. The wait lock gets reset if it takes to
   * long to shrink or grow for whatever reason.
   */
  void obtainPlaceLoad() {
    while (placeActive) {
      if (GlobalRuntimeImpl.getRuntime().EVOLVING.waitingForPlaceChanges) {
        // Thread is waiting for a new place from scheduler
        // check if timeout is reached and reset lock
        now = System.nanoTime();
        if (((now - startedWaiting) / 1e6) > timeoutWaitForPlaceChanges) {
          GlobalRuntimeImpl.getRuntime().EVOLVING.waitingForPlaceChanges = false;
        }
      } else {
        try {
          loadPlace = load.getLoad();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        final double _loadPlace = loadPlace;
        final int _id = here().id;
        if (placeActive) {
          try {
            GlobalRuntimeImpl runtime = GlobalRuntimeImpl.getRuntime();
            if (runtime != null) {
              runtime.immediateAsyncAt(
                  place(0),
                  () -> {
                    GlobalRuntimeImpl.getRuntime().EVOLVING.loadMapRef.get().put(_id, _loadPlace);
                  });
            }
          } catch (Throwable t) {
            t.printStackTrace(System.out);
          }
        } else {
          break;
        }
      }
      try {
        Thread.sleep(loadCheckInterval);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
