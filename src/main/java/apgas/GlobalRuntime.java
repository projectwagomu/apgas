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

package apgas;

import apgas.impl.GlobalRuntimeImpl;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * The {@link GlobalRuntime} class provides mechanisms to initialize and shut down the APGAS global
 * runtime for the application.
 *
 * <p>The global runtime is implicitly initialized when first used. The current runtime can be
 * obtained from the {@link #getRuntime()} method, which forces initialization.
 */
public abstract class GlobalRuntime {

  /** Indicates if all system-wide instances are ready, only used on place 0 */
  public static AtomicInteger readyCounter;

  /** The command line arguments if the main method of this class is invoked. */
  private static String[] args;

  /** Constructs a new {@link GlobalRuntime} instance. */
  protected GlobalRuntime() {}

  /**
   * Returns the {@link GlobalRuntime} instance for this place.
   *
   * @return the GlobalRuntime instance
   */
  public static GlobalRuntime getRuntime() {
    return getRuntimeImpl();
  }

  /**
   * Returns the {@link GlobalRuntimeImpl} instance for this place.
   *
   * @return the GlobalRuntimeImpl instance
   */
  static GlobalRuntimeImpl getRuntimeImpl() {
    final GlobalRuntimeImpl runtime = GlobalRuntimeWrapper.runtime;
    while (!runtime.ready) { // Wait for constructor
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
      }
    }
    return runtime;
  }

  /**
   * Initializes the global runtime.
   *
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    GlobalRuntime.args = args;
    getRuntime();
  }

  /**
   * Returns the executor service for the place.
   *
   * @return the executor service
   */
  public abstract ExecutorService getExecutorService();

  /**
   * Returns the time of the last place failure as reported by {@link System#nanoTime} if any or
   * null otherwise.
   *
   * @return the time of the last place failure or null
   */
  public abstract Long lastfailureTime();

  /**
   * Registers a place failure handler.
   *
   * <p>The handler is invoked for each failed place.
   *
   * @param handler the handler to register or null to unregister the current handler
   */
  public abstract void setPlaceFailureHandler(Consumer<Place> handler);

  /**
   * Registers a runtime failure handler.
   *
   * <p>The handler is invoked on runtime shutdown. Irregular runtime shutdown due to losing
   * Hazelcast partitions may also invoke this handler.
   *
   * @param handler the handler to register or null to unregister the current handler
   */
  public abstract void setRuntimeShutdownHandler(Runnable handler);

  /** Shuts down the {@link GlobalRuntime} instance. */
  public abstract void shutdown();

  /** A wrapper class for implementing the singleton pattern. */
  private static class GlobalRuntimeWrapper {

    /** The {@link GlobalRuntime} instance for this place. */
    private static final GlobalRuntimeImpl runtime = new GlobalRuntimeImpl(args);
  }
}
