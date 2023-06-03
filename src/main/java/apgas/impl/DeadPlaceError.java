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

/**
 * A {@link DeadPlaceError} is thrown when attempting a finish state change from
 * a place that is considered dead by the finish.
 */
final class DeadPlaceError extends Error {

	private static final long serialVersionUID = -6291716310951978192L;

	/** Constructs a new {@link DeadPlaceError}. */
	DeadPlaceError() {
	}
}
