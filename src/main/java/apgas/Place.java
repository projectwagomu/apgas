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

import java.io.Serializable;

/** The {@link Place} class represents an APGAS place. */
public class Place implements Serializable, Comparable<Place> {

	private static final long serialVersionUID = -7210312031258955537L;

	/** The integer ID of this place. */
	public final int id;

	/**
	 * Constructs a {@link Place} with the specified ID.
	 *
	 * @param id the desired place ID
	 * @throws IllegalArgumentException if the argument is negative
	 */
	public Place(int id) {
		if (id < 0) {
			throw new IllegalArgumentException("Illegal place id: " + id);
		}
		this.id = id;
	}

	@Override
	public int compareTo(Place o) {
		return Integer.compare(id, o.id);
	}

	@Override
	public boolean equals(Object that) {
		return that instanceof Place && id == ((Place) that).id;
	}

	@Override
	public int hashCode() {
		return Integer.hashCode(id);
	}

	@Override
	public String toString() {
		return "place(" + id + ")";
	}
}
