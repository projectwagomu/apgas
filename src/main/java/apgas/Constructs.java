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

import apgas.impl.SerializableRunnable;
import apgas.impl.Worker;
import apgas.impl.elastic.EvolvingHandler;
import apgas.impl.elastic.GetLoad;
import apgas.impl.elastic.MalleableHandler;
import com.hazelcast.core.Member;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;

/** The {@link Constructs} class defines the APGAS constructs by means of static methods. */
public final class Constructs {

  /** Prevents instantiation. */
  private Constructs() {}

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
   * Forks a new local task to the local pool of the calling worker, with body {@code f} and returns
   * immediately.
   *
   * @param f the function to run
   */
  public static void asyncFork(SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().asyncFork(f);
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
   * Method used to define the malleable handler which will be responsible for interacting with the
   * running program so that it correctly handles the transitions between place count changes. The
   * program becomes malleable as a result of calling this method. This is because a socket is
   * opened as a result of this call, effectively making it possible to receive shrink of grow
   * orders from the job scheduler.
   *
   * @param handler the handler now in charge of handling malleable shrink and grow orders from the
   *     job scheduler
   */
  public static void defineMalleableHandler(MalleableHandler handler) {
    GlobalRuntime.getRuntimeImpl().setMalleableHandler(handler);
  }

  /**
   * Method used to define the evolving handler which will be responsible for interacting with the
   * running program so that it correctly handles the transitions between place count changes. The
   * program becomes evolving as a result of calling this method. This is because a socket is opened
   * as a result of this call, effectively making it possible to send shrink of grow requests to the
   * job scheduler.
   *
   * @param handler the handler now in charge of sending evolving shrink and grow requests to the
   *     job scheduler
   */
  public static void defineEvolvingHandler(EvolvingHandler handler, GetLoad getLoad) {
    GlobalRuntime.getRuntimeImpl().setEvolvingHandler(handler, getLoad);
  }

  /** Disables the elastic communicator, i.e., the program is no longer elastic. */
  public static void disableElasticCommunicator() {
    GlobalRuntime.getRuntimeImpl().disableElasticCommunicator();
  }

  /**
   * Sends a message to the external scheduler process
   *
   * @param message
   */
  public static void sendToScheduler(final String message) {
    GlobalRuntime.getRuntimeImpl().sendToScheduler(message);
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
   * Returns the current worker
   *
   * @return the worker place
   */
  public static Worker getCurrentWorker() {
    return GlobalRuntime.getRuntimeImpl().getCurrentWorker();
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
   * Runs {@code f} at {@link Place} {@code p} immediately
   *
   * @param member the hazelcast member of execution
   * @param f the function to run
   */
  public static void immediateAsyncAt(Member member, SerializableRunnable f) {
    GlobalRuntime.getRuntimeImpl().immediateAsyncAt(member, f);
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
   * Returns the liveness of a place
   *
   * @param place the place whose state is to be checked
   * @return isDead
   */
  public static boolean isDead(Place place) {
    return GlobalRuntime.getRuntimeImpl().isDead(place);
  }

  /**
   * Returns the next place
   *
   * @param place the place whose next place should be returned
   * @return the place following the place given as parameter
   */
  public static Place nextPlace(Place place) {
    return GlobalRuntime.getRuntimeImpl().nextPlace(place);
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
   * Returns the previous place
   *
   * @param place the place whose previous place should be returned
   * @return the previous place
   */
  public static Place prevPlace(Place place) {
    return GlobalRuntime.getRuntimeImpl().prevPlace(place);
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
}
