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

package apgas.launcher;

import java.util.Collections;
import java.util.List;

import apgas.impl.HostManager;

/** The {@link NoLauncher} class does not spawn any place. */
public class NoLauncher implements Launcher {

	@Override
	public boolean healthy() {
		return true;
	}

	@Override
	public List<Integer> launch(HostManager hostManager, int n, boolean verbose) throws Exception {
		if (verbose) {
			System.err.println("[APGAS] Ignoring attempt to spawn " + n + " new place(s), command: "
					+ String.join(" ", hostManager.getCopyOfLaunchCommand()));
		}
		return Collections.emptyList();
	}

	@Override
	public void shutdown() {
	}
}
