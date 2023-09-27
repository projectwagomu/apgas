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

package apgas.launcher;

import apgas.impl.HostManager;
import java.util.List;

/** The {@link Launcher} interface. */
public interface Launcher {

  /**
   * Checks that all the processes launched are healthy.
   *
   * @return true if all subprocesses are healthy
   */
  boolean healthy();

  /**
   * Launches n processes with the given command line and host list. The first host of the list is
   * skipped. If the list is incomplete, the last host is repeated.
   *
   * @param hostManager containing all hosts and generator for place ids
   * @param n number of processes to launch
   * @param verbose dumps the executed commands to stderr
   * @param expectedPlacesCount expected places count after launching was done
   * @throws Exception if launching fails
   * @return the place ids of the newly launched processes
   */
  List<Integer> launch(HostManager hostManager, int n, boolean verbose, int expectedPlacesCount)
      throws Exception;

  /** Shuts down the {@link Launcher} instance. */
  void shutdown();
}
