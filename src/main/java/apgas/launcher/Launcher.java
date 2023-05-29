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

package apgas.launcher;

import apgas.impl.HostManager;
import java.util.List;

/** The {@link Launcher} interface. */
public interface Launcher {

	/**
	 * Launches n processes with the given command line and host list. The first
	 * host of the list is skipped. If the list is incomplete, the last host is
	 * repeated.
	 *
	 * @param hostManager containing all hosts and generator for place ids
	 * @param n           number of processes to launch
	 * @param verbose     dumps the executed commands to stderr
	 * @throws Exception if launching fails
	 * @return the place ids of the newly launched processes
	 */
	List<Integer> launch(HostManager hostManager, int n, boolean verbose) throws Exception;

	/** Shuts down the {@link Launcher} instance. */
	void shutdown();

	/**
	 * Checks that all the processes launched are healthy.
	 *
	 * @return true if all subprocesses are healthy
	 */
	boolean healthy();
}
