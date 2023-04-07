package apgas.runtime;

import static apgas.Constructs.async;
import static apgas.Constructs.asyncAt;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.place;
import static apgas.Constructs.places;
import static apgas.Constructs.shutdownMallPlacesBlocking;
import static apgas.Constructs.startMallPlacesBlocking;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import apgas.Configuration;
import apgas.Constructs;
import apgas.GlobalRuntime;
import apgas.Place;
import apgas.util.GlobalRef;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class APGASCoreTests {

  @BeforeAll
  static void beforeAll() {
    Configuration.APGAS_THREADS.setDefaultValue(5);
    Configuration.APGAS_PLACES.setDefaultValue(6);
    Configuration.APGAS_RESILIENT.set(true);

    GlobalRuntime.getRuntime();
  }

  @AfterAll
  static void afterAll() {}

  @Test
  @DisplayName("Testing Finish with single async.")
  void shouldWaitForSingleAsyncTaskToFinish() {
    AtomicInteger testCounter = new AtomicInteger(0);
    finish(
        () ->
            async(
                () -> {
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                  testCounter.set(1);
                }));
    assertEquals(1, testCounter.get(), "The Counter should be set to 1 by the async Task.");
  }

  @Test
  @DisplayName("Testing nested Finish no Activity.")
  void shouldWaitCorrectlyWithNestedFinish() {
    finish(
        () ->
            finish(
                () -> {
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }));
  }

  @Test
  @DisplayName("Testing nested Finish with asyncAt.")
  void shouldWaitCorrectlyForAsyncAtInsideNestedFinish() {

    List<Place> testPlaces = new ArrayList<>(places());
    testPlaces.remove(Constructs.here());
    Place testPlace = testPlaces.get(0);
    finish(
        () ->
            finish(
                () ->
                    asyncAt(
                        testPlace,
                        () -> {
                          try {
                            Thread.sleep(100);
                          } catch (InterruptedException e) {
                            e.printStackTrace();
                          }
                        })));
  }

  @Test
  @DisplayName("Testing nested Finish with two asyncAt.")
  void shouldWaitCorrectlyForBothAsyncAt() {
    ArrayList<Place> testPlaces = new ArrayList<>(places());
    testPlaces.remove(here());
    Place testPlace = testPlaces.get(0);
    finish(
        () -> {
          asyncAt(
              testPlace,
              () -> {
                try {
                  Thread.sleep(250);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              });
          finish(
              () ->
                  asyncAt(
                      testPlace,
                      () -> {
                        try {
                          Thread.sleep(100);
                        } catch (InterruptedException e) {
                          e.printStackTrace();
                        }
                      }));
        });
  }

  @Test
  @DisplayName("Testing Finish with asyncAt and its back")
  void shouldWaitForAsyncAtAndBack() {
    AtomicInteger testCounter = new AtomicInteger(0);
    GlobalRef<AtomicInteger> globalTestCounter = new GlobalRef<>(testCounter);
    ArrayList<Place> testPlaces = new ArrayList<>(places());
    testPlaces.remove(here());
    assertFalse(testPlaces.isEmpty(), "There should be started at least one more Place.");
    finish(
        () ->
            asyncAt(
                testPlaces.get(0),
                () -> {
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                  asyncAt(
                      globalTestCounter.home(),
                      () -> {
                        globalTestCounter.get().set(42);
                      });
                }));
    assertEquals(
        42, globalTestCounter.get().get(), "globalTestCounter should be set to 42 by the asyncAt.");
    assertEquals(42, testCounter.get(), "testCounter should be set to 42 by the asyncAt.");
  }

  @Test
  @DisplayName("Testing Finish with nested asyncs")
  void shouldWaitForTransitiveSpawnedAsyncTasks() {
    AtomicInteger testCounter = new AtomicInteger(0);
    finish(
        () ->
            async(
                () -> {
                  async(testCounter::incrementAndGet);
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                  testCounter.incrementAndGet();
                  async(testCounter::incrementAndGet);
                }));
    assertEquals(3, testCounter.get(), "testCounter should be incremented 3 time by asyncs.");
  }

  @Test
  @DisplayName("Testing Finish with nested async and asyncAt")
  void shouldScheduleAsyncAtLocally() {
    AtomicInteger testCounter = new AtomicInteger(0);
    finish(
        () ->
            asyncAt(
                here(),
                () -> {
                  async(testCounter::incrementAndGet);
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                  testCounter.incrementAndGet();
                  async(testCounter::incrementAndGet);
                }));
    assertEquals(3, testCounter.get(), "testCounter should be incremented 3 time by asyncs(At).");
  }

  @Test
  @DisplayName("Testing Finish with nested asyncAts")
  void shouldWaitForNestedAsyncAt() {
    AtomicInteger testCounter = new AtomicInteger(0);
    GlobalRef<AtomicInteger> globalTestCounter = new GlobalRef<>(testCounter);
    ArrayList<Place> testPlaces = new ArrayList<>(places());
    testPlaces.remove(here());
    assertFalse(testPlaces.isEmpty(), "There should be started at least one more Place.");
    finish(
        () ->
            asyncAt(
                testPlaces.get(0),
                () -> {
                  asyncAt(
                      globalTestCounter.home(),
                      () -> {
                        async(() -> globalTestCounter.get().incrementAndGet());
                        async(() -> globalTestCounter.get().incrementAndGet());
                        async(() -> globalTestCounter.get().incrementAndGet());
                      });
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                  asyncAt(
                      globalTestCounter.home(),
                      () -> {
                        globalTestCounter.get().incrementAndGet();
                      });

                  asyncAt(
                      globalTestCounter.home(),
                      () -> {
                        globalTestCounter.get().incrementAndGet();
                      });
                }));
    assertEquals(5, testCounter.get(), "The Counter should be set to 5 by the async Task.");
  }

  @Test
  @DisplayName("Testing nested Finish")
  void shouldWaitForCorrectFinish() {
    AtomicInteger testCounter = new AtomicInteger(0);
    GlobalRef<AtomicInteger> globalTestCounter = new GlobalRef<>(testCounter);
    ArrayList<Place> testPlaces = new ArrayList<>(places());
    testPlaces.remove(here());
    assertFalse(testPlaces.isEmpty(), "There should be started at least one more Place.");
    finish(
        () ->
            asyncAt(
                testPlaces.get(0),
                () -> {
                  AtomicInteger innerFinishTestCounter = new AtomicInteger(0);
                  GlobalRef<AtomicInteger> innerTestCounterRef =
                      new GlobalRef<>(innerFinishTestCounter);
                  finish(
                      () ->
                          asyncAt(
                              globalTestCounter.home(),
                              () -> {
                                async(() -> globalTestCounter.get().incrementAndGet());
                                async(() -> globalTestCounter.get().incrementAndGet());
                                async(
                                    () -> {
                                      try {
                                        Thread.sleep(100);
                                      } catch (InterruptedException e) {
                                        e.printStackTrace();
                                      }
                                      globalTestCounter.get().incrementAndGet();
                                      asyncAt(
                                          innerTestCounterRef.home(),
                                          () -> innerTestCounterRef.get().incrementAndGet());
                                    });
                                asyncAt(
                                    innerTestCounterRef.home(),
                                    () -> innerTestCounterRef.get().incrementAndGet());
                              }));
                  assertEquals(
                      2,
                      innerFinishTestCounter.get(),
                      "The Counter should be set to 2 by the async Task.");

                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                  asyncAt(
                      globalTestCounter.home(), () -> globalTestCounter.get().incrementAndGet());
                  asyncAt(
                      globalTestCounter.home(), () -> globalTestCounter.get().incrementAndGet());
                }));
    assertEquals(5, testCounter.get(), "The Counter should be set to 5 by the async Task.");
  }

  @Disabled
  @Test
  @DisplayName("Testing starting places for malleability")
  void shouldStartMallPlaces() {
    final int initialPlacesCount = places().size();
    final int addOnePlace = 1;
    startMallPlacesBlocking(addOnePlace, false);
    int currentPlacesCount = places().size();
    assertEquals(initialPlacesCount + addOnePlace, currentPlacesCount);
    final int addTwoPlaces = 2;
    startMallPlacesBlocking(addTwoPlaces, false);

    finish(
        () -> {
          for (final Place p : places()) {
            asyncAt(
                p,
                () -> {
                  int currentPlacesCountRemote = places().size();
                  assertEquals(
                      initialPlacesCount + addOnePlace + addTwoPlaces, currentPlacesCountRemote);
                  int i = 0;
                  for (final Place pInside : places()) {
                    assertEquals(i, pInside.id);
                    i++;
                  }
                });
          }
        });

    final int removeFirstPlaceId = places().size() / 2;
    final Place removeFirstPlace = place(removeFirstPlaceId);
    shutdownMallPlacesBlocking(List.of(removeFirstPlace), false);
    currentPlacesCount = places().size();
    assertEquals(initialPlacesCount + addOnePlace + addTwoPlaces - 1, currentPlacesCount);

    final int removeSecondPlaceId = 1;
    final Place removeSecondPlace = place(removeSecondPlaceId);
    final int removeThirdPlaceId = places().size() - 1;
    final Place removeThirdPlace = place(removeThirdPlaceId);
    shutdownMallPlacesBlocking(List.of(removeSecondPlace, removeThirdPlace), false);
    finish(
        () -> {
          for (final Place p : places()) {
            asyncAt(
                p,
                () -> {
                  int currentPlacesCountRemote = places().size();
                  System.out.println(
                      here() + " currentPlacesCountRemote=" + currentPlacesCountRemote);
                  assertEquals(
                      initialPlacesCount + addOnePlace + addTwoPlaces - 3,
                      currentPlacesCountRemote);
                  assertFalse(
                      places().contains(removeFirstPlace),
                      here() + " has still " + removeFirstPlace + " in its places()");
                  assertFalse(
                      places().contains(removeSecondPlace),
                      here() + " has still " + removeSecondPlace + " in its places()");
                  assertFalse(
                      places().contains(removeThirdPlace),
                      here() + " has still " + removeThirdPlace + " in its places()");
                });
          }
        });
  }
}
