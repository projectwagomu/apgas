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
 * The {@link Cell} class implements a mutable generic container.
 *
 * @param <T> the type of the contained object
 */
public final class Cell<T> {

	/** The contained object. */
	private T t;

	/**
	 * Returns the object in this {@link Cell} instance.
	 *
	 * @return the contained object
	 */
	public T get() {
		return t;
	}

	/**
	 * Sets the object in this {@link Cell} instance.
	 *
	 * @param t an object
	 */
	public void set(T t) {
		this.t = t;
	}
}
