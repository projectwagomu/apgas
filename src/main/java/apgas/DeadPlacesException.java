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

import java.util.Collection;

/**
 * A {@link DeadPlacesException} is a {@link MultipleException} caused only by
 * dead place exceptions.
 */
public class DeadPlacesException extends MultipleException {

	private static final long serialVersionUID = 1904057943221698941L;

	/**
	 * Constructs a new {@link DeadPlacesException} from the specified
	 * {@code exceptions}.
	 *
	 * @param exceptions the uncaught exceptions that contributed to this
	 *                   {@code MultipleException}
	 */
	DeadPlacesException(Collection<Throwable> exceptions) {
		super(exceptions);
	}
}
