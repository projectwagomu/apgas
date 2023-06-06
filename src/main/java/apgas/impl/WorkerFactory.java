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

/**
 * The {@link WorkerFactory} class implements a thread factory for the thread
 * pool.
 */
final class WorkerFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {

	private int counter = 0;

	@Override
	public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
		return new Worker(pool, counter++);
	}
}
