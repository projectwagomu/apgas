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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

/** The {@link Worker} class implements a worker thread. */
public final class Worker extends ForkJoinWorkerThread {

  private final int myID;

  /** The current task. */
  Task task;

  /**
   * Instantiates a Worker operating in the given pool.
   *
   * @param pool the pool this worker works in
   * @param id integer identifier for this worker
   */
  Worker(ForkJoinPool pool, int id) {
    super(pool);
    myID = id;
  }

  /**
   * Obtain the id for this worker
   *
   * @return this id of this worker
   */
  public int getMyID() {
    return myID;
  }
}
