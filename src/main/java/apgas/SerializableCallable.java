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
import java.util.concurrent.Callable;

/**
 * A generic serializable functional interface with no arguments and a return
 * value.
 *
 * <p>
 * The functional method is {@link #call()}.
 *
 * @param <T> the type of the result
 */
@FunctionalInterface
public interface SerializableCallable<T> extends Serializable, Callable<T> {

	@Override
	T call() throws Exception;
}
