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
import apgas.DeadPlaceException;
import apgas.Place;
import apgas.util.BadPlaceException;
import apgas.util.ConsolePrinter;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.EndpointQualifier;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** The {@link Transport} class manages the Hazelcast cluster and implements active messages. */
public class Transport implements InitialMembershipListener {

  private static final String APGAS = "apgas";
  private static final String APGAS_EXECUTOR = "apgas:executor";
  private static final String APGAS_FINISH = "apgas:finish";
  private static final String APGAS_PLACE_ID = "apgas:place:id";
  private static final String APGAS_PLACES = "apgas:places";

  /** Hazelcast config */
  private final Config config;

  /** The place ID for this JVM. */
  private final int here;

  /** The current members indexed by place ID. */
  private final Map<Integer, Member> mapPlaceIDtoMember = new ConcurrentHashMap<>();

  /** The global runtime instance to notify of new and dead places. */
  private final GlobalRuntimeImpl runtime;

  /** The Hazelcast instance for this JVM. */
  HazelcastInstance hazelcast;

  /** Executor service for sending active messages. */
  private IExecutorService executor;

  /** The first unused place ID. */
  private int maxPlace;

  /** The local member. */
  private Member me;

  /** Registration ID. */
  private String regMembershipListener;

  /**
   * Initializes the {@link HazelcastInstance} for this global runtime instance.
   *
   * @param runtime the global runtime instance
   * @param master member to connect to or null
   * @param localhost the preferred ip address of this host or null
   * @param backupCount number of backups to use for distributed data structures
   * @param placeId integer identifier of this process (same as place id)
   */
  protected Transport(
      GlobalRuntimeImpl runtime, String master, String localhost, int backupCount, int placeId) {
    this.runtime = runtime;
    // config
    config = new Config();
    config.getMemberAttributeConfig().setIntAttribute(APGAS_PLACE_ID, placeId);
    here = placeId;
    config.setProperty("hazelcast.logging.type", "none");
    config.setProperty("hazelcast.wait.seconds.before.join", "0");
    config.setProperty("hazelcast.socket.connect.timeout.seconds", "1");

    // Partition count must be the same for all hazelcast members
    // Thus, we use this only for fixed runs, i.e., especially for fault tolerance
    if (Configuration.CONFIG_APGAS_ELASTIC.equals(Configuration.APGAS_ELASTIC_FIXED)) {
      config.setProperty(
          "hazelcast.partition.count", String.valueOf(Configuration.CONFIG_APGAS_PLACES.get()));
    }

    final NetworkConfig networkConfig = config.getNetworkConfig();

    final String networkInterface = Configuration.CONFIG_APGAS_NETWORK_INTERFACE.get();
    if (networkInterface != null && networkInterface.length() > 0) {
      System.err.println("[APGAS] sets network interface to " + networkInterface);
      networkConfig.getInterfaces().setEnabled(true).addInterface(networkInterface);
    }

    config.addMapConfig(
        new MapConfig(APGAS_FINISH)
            .setInMemoryFormat(InMemoryFormat.OBJECT)
            .setBackupCount(backupCount));

    // join config
    final JoinConfig join = config.getNetworkConfig().getJoin();
    join.getMulticastConfig().setEnabled(false);
    join.getTcpIpConfig().setEnabled(true);
    if (localhost != null) {
      System.setProperty("hazelcast.local.localAddress", localhost);
    }
    if (master != null) {
      join.getTcpIpConfig().addMember(master);
    }
    config.setInstanceName(APGAS);
    config.addListConfig(new ListConfig(APGAS_PLACES).setBackupCount(backupCount));
  }

  /**
   * adds a new Member from Hazelcast as Place to the local PlaceMap
   *
   * @param member The Member of Hazelcast to add as Place to the Transport
   */
  private void addPlace(Member member) {
    final Integer placeID = member.getIntAttribute(APGAS_PLACE_ID);
    if (mapPlaceIDtoMember.containsKey(placeID)) {
      System.err.println("[APGAS] a new place was added but ID is already in use: " + placeID);
      throw new BadPlaceException();
    }
    maxPlace = Math.max(maxPlace, placeID + 1);
    mapPlaceIDtoMember.put(placeID, member);
    final List<Integer> added = new ArrayList<>();
    added.add(placeID);
    runtime.updatePlaces(added, new ArrayList<>());
  }

  /**
   * Returns the socket address of this Hazelcast instance.
   *
   * @return an address in the form "ip:port"
   */
  protected String getAddress() {
    final InetSocketAddress address = me.getSocketAddress(EndpointQualifier.MEMBER);
    return address.getAddress().getHostAddress() + ":" + address.getPort();
  }

  /**
   * Returns the distributed map instance with the given name.
   *
   * @param <K> key type
   * @param <V> value type
   * @param name map name
   * @return the map
   */
  <K, V> IMap<K, V> getMap(String name) {
    return hazelcast.getMap(name);
  }

  /**
   * Provides a map from integers to member objects
   *
   * @return map
   */
  public Map<Integer, Member> getMembers() {
    return mapPlaceIDtoMember;
  }

  /**
   * Returns the distributed map instance implementing resilient finish.
   *
   * @param <K> key type
   * @param <V> value type
   * @return the map
   */
  <K, V> IMap<K, V> getResilientFinishMap() {
    return hazelcast.getMap(APGAS_FINISH);
  }

  /**
   * Returns the current place ID.
   *
   * @return the place ID of this Hazelcast instance
   */
  protected int here() {
    return here;
  }

  @Override
  public synchronized void init(InitialMembershipEvent event) {
    event.getMembers().forEach(this::addPlace);
  }

  /**
   * Returns the first unused place ID.
   *
   * @return a place ID.
   */
  protected int maxPlace() {
    return maxPlace;
  }

  @Override
  public synchronized void memberAdded(MembershipEvent membershipEvent) {
    addPlace(membershipEvent.getMember());
  }

  @Override
  public synchronized void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
    // TODO Jonas weird "a new place was added?"
    ConsolePrinter.getInstance().printlnAlways("weird memberAttributeChanged call from Hazelcast");
    ConsolePrinter.getInstance().printlnAlways(memberAttributeEvent.toString());
    addPlace(memberAttributeEvent.getMember());
  }

  @Override
  public synchronized void memberRemoved(MembershipEvent membershipEvent) {
    this.removePlace(membershipEvent.getMember());
  }

  /**
   * Removes the place with the specified id from the distributed runtime.
   *
   * @param placeID the id of the place to remove
   */
  public void removePlace(int placeID) {
    mapPlaceIDtoMember.remove(placeID);
    final List<Integer> removed = new ArrayList<>();
    removed.add(placeID);
    runtime.updatePlaces(new ArrayList<>(), removed);
  }

  /**
   * Method used to remove a member of the cluster from the transport layer
   *
   * @param member the member to remove
   */
  public void removePlace(Member member) {
    final Integer placeID = member.getIntAttribute(APGAS_PLACE_ID);
    removePlace(placeID);
  }

  /**
   * Executes a function at the given place.
   *
   * @param place the requested place of execution
   * @param f the function to execute
   * @throws DeadPlaceException if the cluster does not contain this place
   */
  protected void send(int place, SerializableRunnable f) {
    if (place == here) {
      f.run();
    } else {
      final Member member = mapPlaceIDtoMember.get(place);
      if (member == null) {
        System.out.println("[APGAS] Exception: cannot send to place " + place);
        throw new DeadPlaceException(new Place(place));
      }
      executor.executeOnMember(f, member);
    }
  }

  /**
   * Executes a function at the given member.
   *
   * @param member the requested place of execution
   * @param f the function to execute
   * @throws DeadPlaceException if the cluster does not contain this place
   */
  protected void send(Member member, SerializableRunnable f) {
    if (member == null) {
      System.out.println("[APGAS] Exception: cannot send to member " + member);
      throw new DeadPlaceException(new Place(Integer.MIN_VALUE));
    }
    if (member.equals(me)) {
      f.run();
    } else {
      executor.executeOnMember(f, member);
    }
  }

  /** Shuts down this Hazelcast instance. */
  protected synchronized void shutdown() {
    hazelcast.getCluster().removeMembershipListener(regMembershipListener);
    hazelcast.shutdown();
  }

  /** Starts monitoring cluster membership events. */
  protected synchronized void start() {
    // regItemListener = allMembers.addItemListener(this, false);
    regMembershipListener = hazelcast.getCluster().addMembershipListener(this);
  }

  /** Start the Hazelcast library to establish connection with the other processes in the runtime */
  public void startHazelcast() {
    try {
      hazelcast = Hazelcast.newHazelcastInstance(config);
      me = hazelcast.getCluster().getLocalMember();
      executor = hazelcast.getExecutorService(APGAS_EXECUTOR);
    } catch (final Throwable t) {
      System.err.println(
          "[APGAS] startHazelcast: "
              + ManagementFactory.getRuntimeMXBean().getName()
              + " throws Exception");
      t.printStackTrace();
    }
  }
}
