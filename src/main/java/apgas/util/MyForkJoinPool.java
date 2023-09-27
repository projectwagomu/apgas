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
package apgas.util;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/** Custom pool used to manage the APGAS activities */
public class MyForkJoinPool extends ForkJoinPool {
  /**
   * Constructor
   *
   * @param parallelism pool parallelism
   * @param maxParallelism maximum number of threads
   * @param factory thread factory for this pool
   * @param handler handler in case the running thread does not terminate correctly
   */
  public MyForkJoinPool(
      int parallelism,
      int maxParallelism,
      ForkJoinWorkerThreadFactory factory,
      Thread.UncaughtExceptionHandler handler) {
    super(
        parallelism,
        factory,
        handler,
        false,
        parallelism,
        maxParallelism,
        1,
        null,
        60_000L,
        TimeUnit.MILLISECONDS);
  }
}
