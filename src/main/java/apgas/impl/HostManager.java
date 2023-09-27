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

import apgas.Configuration;
import apgas.Place;
import apgas.util.ConsolePrinter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This Class maintains all Hosts of the Runtime to reuse the Hosts after removing some of the
 * Places. ATM we assume that the initially given list of hosts are reachable and usable until the
 * end of the whole execution.
 */
public class HostManager {

  /** Command to start new Places */
  final ArrayList<String> launchCommand = new ArrayList<>();

  final int numFirstPlaces = Configuration.CONFIG_APGAS_PLACES.get();

  /** The List of Hosts */
  private final List<Host> hosts;

  /**
   * This atomic Integer is used to generate unique ID's for the Places. Start counting on 1,
   * because the Master node gets the value 0 as default
   */
  private final AtomicInteger placeIDGenerator;

  private final int strategy;
  private final boolean verboseLauncher = Configuration.CONFIG_APGAS_VERBOSE_LAUNCHER.get();
  String removedHostName = null;

  /**
   * Constructor
   *
   * @param hostnames The List of Hostnames to maintain
   * @param localhost the name of the local host
   */
  public HostManager(final List<String> hostnames, final String localhost) {
    // We assume that the first host in hostname is the localhost (master)
    hosts = new ArrayList<>();
    if (hostnames.size() > 0) {
      for (final String hostname : hostnames) {
        addHost(hostname);
      }
      // for (Host h : this.hosts) h.maxPlacesPerHost = Integer.MAX_VALUE;

      hosts.get(0).attachPlace(new Place(0));
    } else if (localhost != null) {
      final Host host = new Host(localhost, numFirstPlaces);
      host.attachPlace(new Place(0));
      hosts.add(host);
    } else {
      System.err.println("[APGAS] HostManager: Error! No valid hostnames and no localhost!");
    }
    placeIDGenerator = new AtomicInteger(0);

    // this.strategy = Configuration.APGAS_HOSTMANAGER_STRATEGY.get();
    strategy = 0;
  }

  /**
   * Adds a host to the list of hosts being managed. If the hostname given as parameter is already
   * contained in the managed list, the value of maxPlacesPerHost is incremented
   *
   * @param hostname name of the new host susceptible to be used to launch new places
   */
  public void addHost(String hostname) {
    for (final Host host : hosts) {
      if (host.hostName.equals(hostname)) {
        host.maxPlacesPerHost++;
        return;
      }
    }
    hosts.add(new Host(hostname, 1));
  }

  //  public void addHostFromMessage(SchedulerMessages message) {
  //    List<String> newHostNames = message.getHostNames();
  //    for (String newHostname : newHostNames) {
  //      addHost(newHostname);
  //    }
  //  }

  public void buildLaunchCommand(String masterIp, String classToStart) {
    launchCommand.add(Configuration.CONFIG_APGAS_JAVA.get());
    final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    final List<String> inputArguments = runtimeMXBean.getInputArguments();
    for (final String a : inputArguments) {
      if (!a.contains("-Dapgas") && !a.contains("-Dglb")) {
        launchCommand.add(a);
      }
    }

    launchCommand.add("-Duser.dir=" + System.getProperty("user.dir"));
    launchCommand.add("-cp");
    launchCommand.add(System.getProperty("java.class.path"));
    for (final String property : System.getProperties().stringPropertyNames()) {
      if (property.startsWith("apgas.") && !property.contains(".place.id")
          || property.startsWith("glb")) {
        launchCommand.add("-D" + property + "=" + System.getProperty(property));
      }
    }
    launchCommand.add(
        "-D"
            + Configuration.CONFIG_APGAS_MASTER.getName()
            + "="
            + masterIp
            + ":"
            + Configuration.CONFIG_APGAS_PORT.get());
    launchCommand.add(classToStart);
  }

  /**
   * detaches the given Place from the corresponding Host
   *
   * @param place The Place which should be detached from its Host
   * @return name of the host hosting the place up until now
   */
  public String detachFromHost(final Place place) {
    // findFirst should find always one or no entry, because a Place can only have
    // one Host at a Time
    final Optional<Host> first =
        hosts.stream().filter(host -> host.attachedPlaces.contains(place)).findFirst();
    removedHostName = null;
    first.ifPresent(
        host -> {
          host.detachPlace(place);
          ConsolePrinter.getInstance().printlnWithoutAPGAS(place + " detached from " + host);
          removedHostName = host.getHostName();
        });
    return removedHostName;
  }

  /**
   * generates the given amount of new PlaceID's
   *
   * @param count The amount of PlaceID's to generate
   * @return The List of generated PlaceID's
   */
  public List<Integer> generateNewPlaceIds(int count) {
    return Stream.generate(() -> placeIDGenerator.incrementAndGet())
        .limit(count)
        .collect(Collectors.toList());
  }

  public List<String> getCopyOfLaunchCommand() {
    final List<String> result = new ArrayList<>();
    for (final String c : launchCommand) {
      result.add(c);
    }
    return result;
  }

  /**
   * gets all HostNames
   *
   * @return a List of all HostNames
   */
  public List<String> getHostNames() {
    final List<String> result = new ArrayList<>();
    for (final Host h : hosts) {
      result.add(h.hostName);
    }
    return result;
  }

  /**
   * retrieve the next possible Host to use for a new Place.
   *
   * @return next suitable Host to use for a new Place, could be null
   */
  public Host getNextHost() {
    Host nexHost = null;
    if (strategy == 1) { // Nodes are filled one after the other
      for (final Host h : hosts) {
        if (h.attachedPlacesCount() < h.maxPlacesPerHost) {
          nexHost = h;
          break;
        }
      }
    } else if (strategy == 0) { // (stragety == 0) Places are added cyclical to the nodes (default)
      int difference = 0;
      for (final Host h : hosts) {
        final int diff = h.maxPlacesPerHost - h.attachedPlacesCount();
        if (diff > 0 && diff > difference) {
          difference = diff;
          nexHost = h;
        }
      }
    }
    if (verboseLauncher) {
      System.err.println("Hostmanager: getNextHost found host: " + nexHost.getHostName());
    }
    return nexHost;
  }

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("HostManager:\n");
    for (final Host host : hosts) {
      stringBuilder.append(" " + host + "\n");
    }
    stringBuilder.append(" placeIDGenerator=" + placeIDGenerator.get());
    return stringBuilder.toString();
  }

  /** This Class represents a Host with a Hostname and it's attached Places. */
  public static class Host {

    private final List<Place> attachedPlaces;
    private final String hostName;
    int maxPlacesPerHost;

    /**
     * Constructor
     *
     * @param hostName hostname of the Host
     * @param maxPlacesPerHost maximum number of places that can be allocated per host
     */
    public Host(final String hostName, final int maxPlacesPerHost) {
      this.hostName = hostName;
      attachedPlaces = new ArrayList<>();
      this.maxPlacesPerHost = maxPlacesPerHost;
    }

    /**
     * get the number of attached Hosts
     *
     * @return the number of attached Places
     */
    public int attachedPlacesCount() {
      return attachedPlaces.size();
    }

    /**
     * attaches the given Place to this Host
     *
     * @param place The Place to attach to this Host
     */
    public void attachPlace(final Place place) {
      if (attachedPlacesCount() >= maxPlacesPerHost) {
        System.err.println(
            "[APGAS] HostManager: on Host "
                + hostName
                + " are now "
                + (attachedPlacesCount() + 1)
                + " attached, but maxPlacesPerHost="
                + maxPlacesPerHost);
      }
      attachedPlaces.add(place);
      ConsolePrinter.getInstance().printlnWithoutAPGAS(place + " attached from " + this);
    }

    /**
     * detaches the given Place from this Host
     *
     * @param place The Place to detach from this Host
     */
    public void detachPlace(final Place place) {
      attachedPlaces.remove(place);
      ConsolePrinter.getInstance().printlnWithoutAPGAS(place + "detached from " + this);
      maxPlacesPerHost--;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Host host = (Host) o;
      return hostName.equals(host.hostName);
    }

    /**
     * get the Hostname of this Host
     *
     * @return the Hostname
     */
    public String getHostName() {
      return hostName;
    }

    @Override
    public int hashCode() {
      return Objects.hash(hostName);
    }

    @Override
    public String toString() {
      return "hostName="
          + hostName
          + ", attachedPlaces="
          + attachedPlaces
          + ", maxPlacesPerHost="
          + maxPlacesPerHost;
    }
  }
}
