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

package apgas.util;

import apgas.Place;

/**
 * A {@link BadPlaceException} is thrown by a {@link GlobalRef} instance when
 * accessed from {@link Place} where it is not defined.
 */
public class BadPlaceException extends RuntimeException {

	private static final long serialVersionUID = 8639251079580877933L;

	/** Constructs a new {@link BadPlaceException}. */
	public BadPlaceException() {
	}
}
