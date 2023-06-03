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
package apgas.runtime;

import static apgas.Constructs.async;
import static apgas.Constructs.asyncAt;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.places;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import apgas.Configuration;
import apgas.Constructs;
import apgas.GlobalRuntime;
import apgas.Place;
import apgas.util.GlobalRef;

public class APGASCoreTests {

	@AfterAll
	static void afterAll() {
	}

	@BeforeAll
	static void beforeAll() {
		Configuration.CONFIG_APGAS_THREADS.setDefaultValue(5);
		Configuration.CONFIG_APGAS_PLACES.setDefaultValue(6);
		Configuration.CONFIG_APGAS_RESILIENT.set(true);

		GlobalRuntime.getRuntime();
	}

	@Test
	@DisplayName("Testing Finish with nested async and asyncAt")
	void shouldScheduleAsyncAtLocally() {
		final AtomicInteger testCounter = new AtomicInteger(0);
		finish(() -> asyncAt(here(), () -> {
			async(testCounter::incrementAndGet);
			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
			testCounter.incrementAndGet();
			async(testCounter::incrementAndGet);
		}));
		assertEquals(3, testCounter.get(), "testCounter should be incremented 3 time by asyncs(At).");
	}

	@Test
	@DisplayName("Testing nested Finish with asyncAt.")
	void shouldWaitCorrectlyForAsyncAtInsideNestedFinish() {

		final List<Place> testPlaces = new ArrayList<>(places());
		testPlaces.remove(Constructs.here());
		final Place testPlace = testPlaces.get(0);
		finish(() -> finish(() -> asyncAt(testPlace, () -> {
			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
		})));
	}

	@Test
	@DisplayName("Testing nested Finish with two asyncAt.")
	void shouldWaitCorrectlyForBothAsyncAt() {
		final ArrayList<Place> testPlaces = new ArrayList<>(places());
		testPlaces.remove(here());
		final Place testPlace = testPlaces.get(0);
		finish(() -> {
			asyncAt(testPlace, () -> {
				try {
					Thread.sleep(250);
				} catch (final InterruptedException e) {
					e.printStackTrace();
				}
			});
			finish(() -> asyncAt(testPlace, () -> {
				try {
					Thread.sleep(100);
				} catch (final InterruptedException e) {
					e.printStackTrace();
				}
			}));
		});
	}

	@Test
	@DisplayName("Testing nested Finish no Activity.")
	void shouldWaitCorrectlyWithNestedFinish() {
		finish(() -> finish(() -> {
			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
		}));
	}

	@Test
	@DisplayName("Testing Finish with asyncAt and its back")
	void shouldWaitForAsyncAtAndBack() {
		final AtomicInteger testCounter = new AtomicInteger(0);
		final GlobalRef<AtomicInteger> globalTestCounter = new GlobalRef<>(testCounter);
		final ArrayList<Place> testPlaces = new ArrayList<>(places());
		testPlaces.remove(here());
		assertFalse(testPlaces.isEmpty(), "There should be started at least one more Place.");
		finish(() -> asyncAt(testPlaces.get(0), () -> {
			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
			asyncAt(globalTestCounter.home(), () -> {
				globalTestCounter.get().set(42);
			});
		}));
		assertEquals(42, globalTestCounter.get().get(), "globalTestCounter should be set to 42 by the asyncAt.");
		assertEquals(42, testCounter.get(), "testCounter should be set to 42 by the asyncAt.");
	}

	@Test
	@DisplayName("Testing nested Finish")
	void shouldWaitForCorrectFinish() {
		final AtomicInteger testCounter = new AtomicInteger(0);
		final GlobalRef<AtomicInteger> globalTestCounter = new GlobalRef<>(testCounter);
		final ArrayList<Place> testPlaces = new ArrayList<>(places());
		testPlaces.remove(here());
		assertFalse(testPlaces.isEmpty(), "There should be started at least one more Place.");
		finish(() -> asyncAt(testPlaces.get(0), () -> {
			final AtomicInteger innerFinishTestCounter = new AtomicInteger(0);
			final GlobalRef<AtomicInteger> innerTestCounterRef = new GlobalRef<>(innerFinishTestCounter);
			finish(() -> asyncAt(globalTestCounter.home(), () -> {
				async(() -> globalTestCounter.get().incrementAndGet());
				async(() -> globalTestCounter.get().incrementAndGet());
				async(() -> {
					try {
						Thread.sleep(100);
					} catch (final InterruptedException e) {
						e.printStackTrace();
					}
					globalTestCounter.get().incrementAndGet();
					asyncAt(innerTestCounterRef.home(), () -> innerTestCounterRef.get().incrementAndGet());
				});
				asyncAt(innerTestCounterRef.home(), () -> innerTestCounterRef.get().incrementAndGet());
			}));
			assertEquals(2, innerFinishTestCounter.get(), "The Counter should be set to 2 by the async Task.");

			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
			asyncAt(globalTestCounter.home(), () -> globalTestCounter.get().incrementAndGet());
			asyncAt(globalTestCounter.home(), () -> globalTestCounter.get().incrementAndGet());
		}));
		assertEquals(5, testCounter.get(), "The Counter should be set to 5 by the async Task.");
	}

	@Test
	@DisplayName("Testing Finish with nested asyncAts")
	void shouldWaitForNestedAsyncAt() {
		final AtomicInteger testCounter = new AtomicInteger(0);
		final GlobalRef<AtomicInteger> globalTestCounter = new GlobalRef<>(testCounter);
		final ArrayList<Place> testPlaces = new ArrayList<>(places());
		testPlaces.remove(here());
		assertFalse(testPlaces.isEmpty(), "There should be started at least one more Place.");
		finish(() -> asyncAt(testPlaces.get(0), () -> {
			asyncAt(globalTestCounter.home(), () -> {
				async(() -> globalTestCounter.get().incrementAndGet());
				async(() -> globalTestCounter.get().incrementAndGet());
				async(() -> globalTestCounter.get().incrementAndGet());
			});
			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
			asyncAt(globalTestCounter.home(), () -> {
				globalTestCounter.get().incrementAndGet();
			});

			asyncAt(globalTestCounter.home(), () -> {
				globalTestCounter.get().incrementAndGet();
			});
		}));
		assertEquals(5, testCounter.get(), "The Counter should be set to 5 by the async Task.");
	}

	@Test
	@DisplayName("Testing Finish with single async.")
	void shouldWaitForSingleAsyncTaskToFinish() {
		final AtomicInteger testCounter = new AtomicInteger(0);
		finish(() -> async(() -> {
			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
			testCounter.set(1);
		}));
		assertEquals(1, testCounter.get(), "The Counter should be set to 1 by the async Task.");
	}

	@Test
	@DisplayName("Testing Finish with nested asyncs")
	void shouldWaitForTransitiveSpawnedAsyncTasks() {
		final AtomicInteger testCounter = new AtomicInteger(0);
		finish(() -> async(() -> {
			async(testCounter::incrementAndGet);
			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
			testCounter.incrementAndGet();
			async(testCounter::incrementAndGet);
		}));
		assertEquals(3, testCounter.get(), "testCounter should be incremented 3 time by asyncs.");
	}
}
