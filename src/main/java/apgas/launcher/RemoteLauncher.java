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

import java.lang.ProcessBuilder.Redirect;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import apgas.Configuration;
import apgas.Place;
import apgas.impl.HostManager;
import apgas.impl.HostManager.Host;

/**
 * The {@link RemoteLauncher} abstract class needs to be extended by specified
 * remote launchers (eg ssh, srun).
 */
public abstract class RemoteLauncher implements Launcher {

	/**
	 * Status of the shutdown sequence (0 live, 1 shutting down the Global Runtime,
	 * 2 shutting down the JVM).
	 */
	private int dying;

	protected Process process;

	ProcessBuilder processBuilder;

	/** The processes we spawned. */
	private final List<Process> processes = new ArrayList<>();

	/** Constructs a new {@link RemoteLauncher} instance. */
	RemoteLauncher() {
		Runtime.getRuntime().addShutdownHook(new Thread(this::terminate));
	}

	@Override
	public boolean healthy() {
		synchronized (this) {
			if (dying > 0) {
				return false;
			}
		}
		for (final Process process : processes) {
			if (!process.isAlive()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public List<Integer> launch(HostManager hostManager, int n, boolean verbose) throws Exception {

		final List<String> command = hostManager.getCopyOfLaunchCommand();

		final String remove = command.remove(command.size() - 1);

		processBuilder = new ProcessBuilder(command);
		processBuilder.redirectOutput(Redirect.INHERIT);
		processBuilder.redirectError(Redirect.INHERIT);
		String hostAddress = null;

		final List<Integer> newPlaceIDs = hostManager.generateNewPlaceIds(n);

		for (final int newPlaceID : newPlaceIDs) {

			final Host host = hostManager.getNextHost();
			if (host == null) {
				System.err.println("[APGAS] Warning: no valid host found; repeating last host: " + host);
			} else {
				hostAddress = host.getHostName();
			}
			host.attachPlace(new Place(newPlaceID));

			if (verbose) {
				System.err.println(hostManager);
			}

			boolean local = false;
			try {
				local = InetAddress.getByName(hostAddress).isLoopbackAddress();
			} catch (final UnknownHostException e) {
				e.printStackTrace();
			}

			command.add("-D" + Configuration.CONFIG_APGAS_PLACE_ID.getName() + "=" + newPlaceID);
			command.add(remove);

			if (local) {
				process = processBuilder.start();
				if (verbose) {
					System.err.println("[APGAS] Spawning new place: " + String.join(" ", command));
				}
			} else {
				startRemote(command, verbose, hostAddress);
			}

			command.remove(command.size() - 1);
			command.remove(command.size() - 1);

			synchronized (this) {
				if (dying <= 1) {
					processes.add(process);
					process = null;
				}
			}
			if (process != null) {
				process.destroyForcibly();
				throw new IllegalStateException("Shutdown in progress");
			}
		}
		return newPlaceIDs;
	}

	@Override
	public synchronized void shutdown() {
		if (dying == 0) {
			dying = 1;
		}
	}

	/** This method must be implemented. */
	abstract void startRemote(List<String> command, boolean verbose, String hostAddress);

	/** Kills all spawned processes. */
	private void terminate() {
		synchronized (this) {
			dying = 2;
		}
		for (final Process process : processes) {
			process.destroyForcibly();
		}
	}
}
