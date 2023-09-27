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
package apgas.impl;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/** The {@link Finish} interface. */
interface Finish extends ForkJoinPool.ManagedBlocker, Serializable {

  /**
   * Reports an uncaught exception to this finish object.
   *
   * @param exception the uncaught exception
   */
  void addSuppressed(Throwable exception);

  @Override
  boolean block();

  /**
   * Must be called exactly once upon completion of the finish.
   *
   * @return the exceptions collected by the finish
   */
  List<Throwable> exceptions();

  @Override
  boolean isReleasable();

  /**
   * Must be called before a task is spawned at place p (local task or outgoing remote task).
   *
   * @param p a place
   */
  void spawn(int p);

  /**
   * Must be called before a task is enqueued for execution (local task or incoming remote task).
   *
   * @param p the place of the parent task
   */
  void submit(int p);

  /** Must be called once a task has completed its execution. */
  void tell();

  /**
   * Must be called to undo the call to {@link #spawn(int)} if the attempt to spawn the task at
   * place p was unsuccessful.
   *
   * @param p a place
   */
  void unspawn(int p);

  /** The abstract {@link Factory} class is the template of all finish factories. */
  abstract class Factory {

    /**
     * Makes a new {@link Finish} instance.
     *
     * @param parent the parent finish object
     * @return the {@link Finish} instance
     */
    abstract Finish make(Finish parent);
  }
}
