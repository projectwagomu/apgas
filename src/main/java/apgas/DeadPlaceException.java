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
 * A {@link DeadPlaceException} is thrown when attempting to interact with a
 * dead {@link Place}.
 */
public class DeadPlaceException extends RuntimeException {

	private static final long serialVersionUID = -4113514316492737844L;

	/** The dead place. */
	public final Place place;

	/**
	 * Constructs a new {@link DeadPlaceException}.
	 *
	 * @param place the dead place
	 */
	public DeadPlaceException(Place place) {
		this.place = place;
	}
}
