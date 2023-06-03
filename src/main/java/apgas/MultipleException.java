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

import static apgas.Constructs.here;

import java.util.Collection;

/**
 * A {@link MultipleException} is thrown by the {@code finish} construct upon
 * termination when the code or tasks in its scope have uncaught exceptions. The
 * uncaught exceptions may be retrieved using the {@link #getSuppressed()}
 * method of this {@link MultipleException}.
 */
public class MultipleException extends RuntimeException {

	private static final long serialVersionUID = 5931977184541245168L;

	/**
	 * Makes a new {@link MultipleException} from the specified {@code exceptions}
	 * with a type reflecting the contained dead place exceptions.
	 *
	 * @param exceptions the uncaught exceptions that contributed to this
	 *                   {@code MultipleException}
	 * @return the exception
	 */
	public static MultipleException make(Collection<Throwable> exceptions) {
		for (final Throwable t : exceptions) {
			System.out.println("[APGAS] " + here() + " Exception occurred");
			t.printStackTrace(System.out);
			if (!(t instanceof DeadPlaceException || t instanceof DeadPlacesException)) {
				return new MultipleException(exceptions);
			}
		}
		return new DeadPlacesException(exceptions);
	}

	/**
	 * Constructs a new {@link MultipleException} from the specified
	 * {@code exceptions}.
	 *
	 * @param exceptions the uncaught exceptions that contributed to this
	 *                   {@code MultipleException}
	 */
	MultipleException(Collection<Throwable> exceptions) {
		for (final Throwable t : exceptions) {
			addSuppressed(t);
		}
	}
}
