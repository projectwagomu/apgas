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

package apgas.launcher;

import apgas.impl.HostManager;
import java.util.Collections;
import java.util.List;

/** The {@link NoLauncher} class does not spawn any place. */
public class NoLauncher implements Launcher {

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

	@Override
	public boolean healthy() {
		return true;
	}
}
