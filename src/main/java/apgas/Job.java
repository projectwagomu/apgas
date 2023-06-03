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

/**
 * A functional interface with no arguments and no return value.
 *
 * <p>
 * The functional method is {@link #run()}.
 */
@FunctionalInterface
public interface Job {

	/**
	 * Runs the function or throws an exception if unable to do so.
	 *
	 * @throws Exception if unable to run the function
	 */
	void run() throws Exception;
}
