/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 */

package apgas;

import apgas.impl.SerializableRunnable;
import apgas.impl.Worker;
import apgas.util.SchedulerMessages;
import com.hazelcast.core.Member;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;

/** The {@link Constructs} class defines the APGAS constructs by means of static methods. */
public final class Constructs {

  /** Prevents instantiation. */
  private Constructs() {}

  /**
   * Runs {@code f} then waits for all tasks transitively spawned by {@code f} to complete.
   *
   * <p>If {@code f} or the tasks transitively spawned by {@code f} have uncaught exceptions then
   * {@code finish(f)} then throws a {@link MultipleException} that collects these uncaught
   * exceptions.
   *
   * @param f the function to run
   * @throws MultipleException if there are uncaught exceptions
   */
  public static void finish(SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().finish(f);
  }

  /**
   * Evaluates {@code f}, waits for all the tasks transitively spawned by {@code f}, and returns the
   * result.
   *
   * <p>If {@code f} or the tasks transitively spawned by {@code f} have uncaught exceptions then
   * {@code finish(F)} then throws a {@link MultipleException} that collects these uncaught
   * exceptions.
   *
   * @param <T> the type of the result
   * @param f the function to run
   * @return the result of the evaluation
   */
  public static <T> T finish(Callable<T> f) {
    return GlobalRuntime.getRuntimeImpl().finish(f);
  }

  /**
   * Forks a new local task to the local pool of the calling worker, with body {@code f} and returns
   * immediately.
   *
   * @param f the function to run
   */
  public static void asyncFork(SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().asyncFork(f);
  }

  /**
   * Submits a new local task to the global runtime with body {@code f} and returns immediately.
   *
   * @param f the function to run
   */
  public static void async(SerializableJob f) {
    asyncAt(here(), f);
  }

  /**
   * Submits a new task to the global runtime to be run at {@link Place} {@code p} with body {@code
   * f} and returns immediately.
   *
   * @param p the place of execution
   * @param f the function to run
   */
  public static void asyncAt(Place p, SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().asyncAt(p, f);
  }

  /**
   * Submits an uncounted task to the global runtime to be run at {@link Place} {@code p} with body
   * {@code f} and returns immediately. The termination of this task is not tracked by the enclosing
   * finish. Exceptions thrown by the task are ignored.
   *
   * @param p the place of execution
   * @param f the function to run
   */
  public static void uncountedAsyncAt(Place p, SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().uncountedAsyncAt(p, f);
  }

  /**
   * Evaluates {@code f} at {@link Place} {@code p}, waits for all the tasks transitively spawned by
   * {@code f}, and returns the result.
   *
   * @param <T> the type of the result (must implement java.io.Serializable)
   * @param p the place of execution
   * @param f the function to run
   * @return the result of the evaluation
   */
  public static <T extends Serializable> T at(Place p, SerializableCallable<T> f) {
    return GlobalRuntime.getRuntimeImpl().at(p, f);
  }

  /**
   * Runs {@code f} at {@link Place} {@code p} and waits for all the tasks transitively spawned by
   * {@code f}.
   *
   * <p>Equivalent to {@code finish(() -> asyncAt(p, f))}
   *
   * @param p the place of execution
   * @param f the function to run
   */
  public static void at(Place p, SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().at(p, f);
  }

  /**
   * Returns the current {@link Place}.
   *
   * @return the current place
   */
  public static Place here() {
    return GlobalRuntime.getRuntimeImpl().here();
  }

  /**
   * Returns the place with the given ID.
   *
   * @param id the requested ID
   * @return the place with the given ID
   */
  public static Place place(int id) {
    return GlobalRuntime.getRuntimeImpl().place(id);
  }

  /**
   * Returns the current list of places in the global runtime.
   *
   * @return the current list of places in the global runtime
   */
  public static List<? extends Place> places() {
    return GlobalRuntime.getRuntimeImpl().places();
  }

  /**
   * Runs {@code f} at {@link Place} {@code p} immediately
   *
   * @param p the place of execution
   * @param f the function to run
   */
  public static void immediateAsyncAt(Place p, SerializableRunnable f) {
    GlobalRuntime.getRuntimeImpl().immediateAsyncAt(p, f);
  }

  /**
   * Runs {@code f} at {@link Place} {@code p} immediately
   *
   * @param member the hazelcast member of execution
   * @param f the function to run
   */
  public static void immediateAsyncAt(Member member, SerializableRunnable f) {
    GlobalRuntime.getRuntimeImpl().immediateAsyncAt(member, f);
  }

  /**
   * Returns the liveness of a place
   *
   * @return isDead
   */
  public static boolean isDead(Place place) {
    return GlobalRuntime.getRuntimeImpl().isDead(place);
  }

  /**
   * Returns the next place
   *
   * @return the next place
   */
  public static Place nextPlace(Place place) {
    return GlobalRuntime.getRuntimeImpl().nextPlace(place);
  }

  /**
   * Returns the previous place
   *
   * @return the previuos place
   */
  public static Place prevPlace(Place place) {
    return GlobalRuntime.getRuntimeImpl().prevPlace(place);
  }

  /**
   * Returns the current worker
   *
   * @return the worker place
   */
  public static Worker getCurrentWorker() {
    return GlobalRuntime.getRuntimeImpl().getCurrentWorker();
  }

  public static void startMallPlaces(int n, boolean verbose) {
    GlobalRuntime.getRuntimeImpl().startMallPlaces(n);
  }

  public static void shutdownMallPlaces(final List<Place> toBeRemoved, boolean verbose) {
    GlobalRuntime.getRuntimeImpl().shutdownMallPlaces(toBeRemoved);
  }

  public static void shutdownMallPlacesBlocking(final List<Place> toBeRemoved, boolean verbose) {
    GlobalRuntime.getRuntimeImpl().shutdownMallPlacesBlocking(toBeRemoved);
  }

  public static SchedulerMessages receiveSchedulerMessage() {
    return GlobalRuntime.getRuntimeImpl().receiveSchedulerMessage();
  }

  public static boolean closeSocket() throws IOException {
    return GlobalRuntime.getRuntimeImpl().closeSocket();
  }

  public static void decrementPlaceIds(List<Place> placesToBeRemoved) {
    GlobalRuntime.getRuntimeImpl().decrementPlaceIds(placesToBeRemoved);
  }
}
