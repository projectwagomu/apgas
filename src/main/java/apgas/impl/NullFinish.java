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

import java.io.Serializable;
import java.util.List;

/** The {@link NullFinish} class implements a dummy finish. */
final class NullFinish implements Serializable, Finish {

	private static final long serialVersionUID = -6486525914605983562L;

	/** The singleton {@link NullFinish} instance. */
	static final NullFinish SINGLETON = new NullFinish();

	private NullFinish() {
	}

	@Override
	public void addSuppressed(Throwable exception) {
	}

	@Override
	public boolean block() {
		return true;
	}

	@Override
	public List<Throwable> exceptions() {
		return null;
	}

	@Override
	public boolean isReleasable() {
		return true;
	}

	@Override
	public void spawn(int p) {
	}

	@Override
	public void submit(int p) {
	}

	@Override
	public void tell() {
	}

	@Override
	public void unspawn(int p) {
	}
}
