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

import java.io.IOException;
import java.util.List;

/** The {@link SrunLauncher} class implements a launcher using srun. */
public class SrunLauncher extends RemoteLauncher {

	@Override
	void startRemote(List<String> command, boolean verbose, String hostAddress) {
		command.add(0, "srun");
		command.add(2, "-N 1");
		command.add(3, "-n 1");
		command.add(4, "-w");
		command.add(5, hostAddress);
		if (verbose) {
			System.err.println("[APGAS] Spawning new place: " + String.join(" ", command));
		}
		try {
			super.process = super.processBuilder.start();
		} catch (final IOException e) {
			e.printStackTrace();
		}
		command.remove(0);
		command.remove(0);
		command.remove(0);
		command.remove(0);
		command.remove(0);
	}
}
