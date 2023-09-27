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

import apgas.Configuration;
import apgas.Place;
import apgas.impl.HostManager;
import apgas.impl.HostManager.Host;
import java.lang.ProcessBuilder.Redirect;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@link RemoteLauncher} abstract class needs to be extended by specified remote launchers (eg
 * ssh, srun).
 */
public abstract class RemoteLauncher implements Launcher {

  /** The processes we spawned. */
  private final List<Process> processes = new ArrayList<>();

  protected Process process;

  ProcessBuilder processBuilder;

  /**
   * Status of the shutdown sequence (0 live, 1 shutting down the Global Runtime, 2 shutting down
   * the JVM).
   */
  private int dying;

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
  public List<Integer> launch(
      HostManager hostManager, int n, boolean verbose, int expectedPlacesCount) throws Exception {

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

      boolean found = false;
      for (int i = 0; i < command.size(); i++) {
        if (command.get(i).contains("-Dapgas.places=")) {
          command.set(i, "-Dapgas.places=" + expectedPlacesCount);
          found = true;
          break;
        }
      }
      if (!found) {
        System.err.println("[APGAS] -Dapgas.places= not found in command list");
      }

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
