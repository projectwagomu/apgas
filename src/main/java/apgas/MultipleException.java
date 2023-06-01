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
