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

package apgas;

import java.util.ArrayList;
import java.util.List;

import apgas.impl.elastic.MalleableCommunicator;
import apgas.impl.elastic.SocketMalleableCommunicator;

/**
 * The {@link Configuration} class defines the names of the system properties used to configure the
 * global runtime.
 *
 * <p>This Class provides only String, Integer, Boolean and Double values at the moment. If you need
 * more, you have to add them manually. In {@code Configuration.get()}.
 */
public final class Configuration<T> {

  /**
   * Places per Node. Needed for all Script Launcher
   *
   * <p>Defaults to 0 (not working)
   */
  public static final String APGAS_PPERNODE_PROPERTY = "apgas.ppernode";

  public static final Configuration<Integer> APGAS_PPERNODE =
      new Configuration<>(APGAS_PPERNODE_PROPERTY, Integer.class);
  /** Absolute Path for writing Scripts in Script Launcher. MUST contain "Remote" */
  public static final String APGAS_SCRIPTNAME_PROPERTY = "apgas.scriptname";

  public static final Configuration<String> APGAS_SCRIPTNAME =
      new Configuration<>(APGAS_SCRIPTNAME_PROPERTY, "RemoteDefaultScriptName", String.class);
  /**
   * Property {@value #APGAS_PLACES_PROPERTY} specifies the desired number of places (Integer
   * property).
   *
   * <p>Defaults to 1. If {@value #APGAS_PLACES_PROPERTY} is not set the global runtime spawns the
   * desired number of places, otherwise it waits for the places to appear.
   */
  private static final String APGAS_PLACES_PROPERTY = "apgas.places";

  public static final Configuration<Integer> APGAS_PLACES =
      new Configuration<>(APGAS_PLACES_PROPERTY, 1, Integer.class);
  /**
   * Property {@value #APGAS_MASTER_PROPERTY} optionally specifies the ip or socket address of the
   * master node (String property).
   *
   * <p>If set to an ip the global runtime connects to the first available global runtime instance
   * at this ip within the default port range.
   */
  private static final String APGAS_MASTER_PROPERTY = "apgas.master";

  public static final Configuration<String> APGAS_MASTER =
      new Configuration<>(APGAS_MASTER_PROPERTY, String.class);
  /**
   * Property {@value #APGAS_HOSTFILE_PROPERTY} specifies a filename that lists hosts on which to
   * launch places (String property).
   */
  private static final String APGAS_HOSTFILE_PROPERTY = "apgas.hostfile";

  public static final Configuration<String> APGAS_HOSTFILE =
      new Configuration<>(APGAS_HOSTFILE_PROPERTY, String.class);
  /**
   * Property {@value #APGAS_RESILIENT_PROPERTY} enables fault tolerance (Boolean property).
   *
   * <p>If set, the global runtime does not abort the execution if a place fails.
   */
  private static final String APGAS_RESILIENT_PROPERTY = "apgas.resilient";

  public static final Configuration<Boolean> APGAS_RESILIENT =
      new Configuration<>(APGAS_RESILIENT_PROPERTY, false, Boolean.class);
  /**
   * Property {@value #APGAS_THREADS_PROPERTY} specifies the desired level of parallelism (Integer
   * property).
   *
   * <p>The return value of {@code Runtime.getRuntime().availableProcessors()} is used if this
   * property is not set.
   */
  private static final String APGAS_THREADS_PROPERTY = "apgas.threads";

  public static final Configuration<Integer> APGAS_THREADS =
      new Configuration<>(
          APGAS_THREADS_PROPERTY, Runtime.getRuntime().availableProcessors(), Integer.class);
  /**
   * Property {@value #APGAS_IMMEDIATE_THREADS_PROPERTY} specifies the desired level of parallelism
   * for the extra immediate thread pool (Integer property).
   *
   * <p>4 is used if this property is not set.
   */
  private static final String APGAS_IMMEDIATE_THREADS_PROPERTY = "apgas.immediate.threads";

  public static final Configuration<Integer> APGAS_IMMEDIATE_THREADS =
      new Configuration<>(APGAS_IMMEDIATE_THREADS_PROPERTY, 4, Integer.class);
  /**
   * Upper bound on the number of persistent threads in the thread pool (Integer property).
   *
   * <p>Defaults to 256.
   */
  private static final String APGAS_MAX_THREADS_PROPERTY = "apgas.max.threads";

  public static final Configuration<Integer> APGAS_MAX_THREADS =
      new Configuration<>(APGAS_MAX_THREADS_PROPERTY, 256, Integer.class);

  /**
   * Specifies the java command to run for spawning places (String property).
   *
   * <p>Defaults to "{@code java}".
   */
  private static final String APGAS_JAVA_PROPERTY = "apgas.java";

  public static final Configuration<String> APGAS_JAVA =
      new Configuration<>(APGAS_JAVA_PROPERTY, "java", String.class);
  /**
   * Controls the verbosity of the launcher (Boolean property).
   *
   * <p>Defaults to "{@code false}".
   */
  private static final String APGAS_VERBOSE_LAUNCHER_PROPERTY = "apgas.verbose.launcher";

  public static final Configuration<Boolean> APGAS_VERBOSE_LAUNCHER =
      new Configuration<>(APGAS_VERBOSE_LAUNCHER_PROPERTY, false, Boolean.class);
  /**
   * Name of the launcher implementation class to instantiate (String property).
   *
   * <p>Defaults to "{@code apgas.launcher.SshLauncher}".
   */
  private static final String APGAS_LAUNCHER_PROPERTY = "apgas.launcher";

  public static final Configuration<String> APGAS_LAUNCHER =
      new Configuration<>(APGAS_LAUNCHER_PROPERTY, "apgas.launcher.SshLauncher", String.class);
  /**
   * Count of backups used by the internal distributed data memory. (Integer property)
   *
   * <p>Set to value between {@code 0} (inclusive) and {@code 6} (inclusive). Defaults to "{@code
   * 1}".
   */
  private static final String APGAS_BACKUPCOUNT_PROPERTY = "apgas.backupcount";

  public static final Configuration<Integer> APGAS_BACKUPCOUNT =
      new Configuration<>(APGAS_BACKUPCOUNT_PROPERTY, 1, Integer.class);
  /**
   * This Property is set by the Master or starting Place to set the id of the Place to start, at
   * launch time. This gives the starting Place (Master atm.) the responsibility of the ID
   * Management.
   */
  private static final String APGAS_PLACE_ID_PROPERTY = "apgas.place.id";

  public static final Configuration<Integer> APGAS_PLACE_ID =
      new Configuration<>(APGAS_PLACE_ID_PROPERTY, 0, Integer.class);

  /** This Property sets the Port of the Master Place. */
  private static final String APGAS_PORT_PROPERTY = "apgas.port";

  public static final Configuration<Integer> APGAS_PORT =
      new Configuration<>(APGAS_PORT_PROPERTY, 5701, Integer.class);

  /**
   * This Property can define the network interface to be used Examples: Kassel FB16: eth0:
   * "141.51.205.*" eth1: "192.168.205.*" ib0: "192.168.169.*"
   *
   * <p>Goethe: eno1: "10.151.*.*" ib0: "10.149.*.*"
   */
  private static final String APGAS_NETWORK_INTERFACE_PROPERTY = "apgas.network.interface";

  public static final Configuration<String> APGAS_NETWORK_INTERFACE =
      new Configuration<>(APGAS_NETWORK_INTERFACE_PROPERTY, String.class);

  /** This Property enables and disables the consoleprinter. */
  private static final String APGAS_CONSOLEPRINTER_PROPERTY = "apgas.consoleprinter";

  public static final Configuration<Boolean> APGAS_CONSOLEPRINTER =
      new Configuration<>(APGAS_CONSOLEPRINTER_PROPERTY, false, Boolean.class);

  /**
   * This Property defines the strategy of the hostmanager: 0 : Places are added cyclical to the
   * nodes (default) 1 : Nodes are filled one after the other. This can result in hosts without
   * places
   */
  private static final String APGAS_HOSTMANAGER_STRATEGY_PROPERTY = "apgas.hostmanager.strategy";

  public static final Configuration<Integer> APGAS_HOSTMANAGER_STRATEGY =
      new Configuration<>(APGAS_HOSTMANAGER_STRATEGY_PROPERTY, 0, Integer.class);

  /**
   * This String is used to define the property setting which indicated if the running APGAS program
   * is elastic. 
   * Possible values are:
   * <ul>
   * <li>fixed: (default), no change in the number of places during execution
   * <li>evolving: (Not implemented yet), the running program may initiate requests for additional places / release some during execution
   * <li>malleable: the running program may dynamically change the number of running places following orders given by the scheduler
   * </ul>
   */
  private static final String APGAS_ELASTIC_PROPERTY = "apgas.elastic";

  /**
   * Possible value for configuration {@link #APGAS_ELASTIC}.
   * This value sets the number of places to remain fixed throughout execution.
   */
  public static final String APGAS_ELASTIC_FIXED = "fixed";
  
  /**
   * Possible value for configuration {@link #APGAS_ELASTIC}.
   * This value sets the runtime to be malleable. A malleable communicator will be prepared by the runtime to receive instructions from the scheduler. 
   * The programmer should call method {@link ExtendedConstructs#defineMalleableHandle(apgas.impl.elastic.MalleableHandler)}
   * to define the actions to perform before and after a malleable change.
   */
  public static final String APGAS_ELASTIC_MALLEABLE = "malleable";
  
  /**
   * Property defining if the program is allowed to change the number of running processes during execution.
   */
  public static final Configuration<String> APGAS_ELASTIC = new Configuration<>(APGAS_ELASTIC_PROPERTY, APGAS_ELASTIC_FIXED, String.class);
  
  /**
   * String used to define the class used as malleable communicator
   */
  private static final String APGAS_MALLEABLE_COMMUNICATOR_PROPERTY = "apgas.malleable_communicator";
  /**
   * Property defining the class used to communicate with the scheduler when a
   * malleable program is running
   */
  public static final Configuration<String> APGAS_MALLEABLE_COMMUNICATOR =
	  new Configuration<>(APGAS_MALLEABLE_COMMUNICATOR_PROPERTY, SocketMalleableCommunicator.class.getCanonicalName(), String.class);
  
  private final String name;
  private final Class<T> propertyType;
  private T defaultValue;
  private T cachedValue;

  /**
   * Constructor
   *
   * @param name The PropertyName of the Configuration Value
   * @param propertyType The Type of the Property-Value
   */
  private Configuration(final String name, final Class<T> propertyType) {
    this.name = name;
    this.propertyType = propertyType;
  }

  /**
   * Constructor
   *
   * @param name The PropertyName of the Configuration Value
   * @param defaultValue A default Value to use if no one is provided via the System-Properties
   * @param propertyType The Type of the Property-Value
   */
  private Configuration(final String name, final T defaultValue, final Class<T> propertyType) {
    this.name = name;
    this.setDefaultValue(defaultValue);
    this.defaultValue = defaultValue;
    this.propertyType = propertyType;
  }

  public static void printAllConfigs() {
    Constructs.finish(
        () -> {
          for (final Place place : Constructs.places()) {
            Constructs.at(
                place,
                () -> {
                  Configuration.printConfigs();
                });
          }
        });
  }

  public static void printConfigs() {
    List<Configuration<?>> allConfigs = new ArrayList<>();
    allConfigs.add(APGAS_PLACES);
    allConfigs.add(APGAS_THREADS);
    allConfigs.add(APGAS_IMMEDIATE_THREADS);
    allConfigs.add(APGAS_MAX_THREADS);
    allConfigs.add(APGAS_RESILIENT);
    allConfigs.add(APGAS_BACKUPCOUNT);
    allConfigs.add(APGAS_PPERNODE);
    allConfigs.add(APGAS_MASTER);
    allConfigs.add(APGAS_HOSTFILE);
    allConfigs.add(APGAS_JAVA);
    allConfigs.add(APGAS_VERBOSE_LAUNCHER);
    allConfigs.add(APGAS_LAUNCHER);
    allConfigs.add(APGAS_PLACE_ID);
    allConfigs.add(APGAS_PORT);
    allConfigs.add(APGAS_NETWORK_INTERFACE);
    allConfigs.add(APGAS_CONSOLEPRINTER);
    allConfigs.add(APGAS_HOSTMANAGER_STRATEGY);
    allConfigs.add(APGAS_ELASTIC);
    allConfigs.add(APGAS_MALLEABLE_COMMUNICATOR);

    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("APGAS config on " + Constructs.here() + ":\n");
    for (final Configuration<?> c : allConfigs) {
      stringBuilder.append("  " + c.getName() + "=" + c.get() + "\n");
    }
    System.out.println(stringBuilder.toString());
  }

  public static void initAll() {
    List<Configuration<?>> allConfigs = new ArrayList<>();
    allConfigs.add(APGAS_PLACES);
    allConfigs.add(APGAS_THREADS);
    allConfigs.add(APGAS_IMMEDIATE_THREADS);
    allConfigs.add(APGAS_MAX_THREADS);
    allConfigs.add(APGAS_RESILIENT);
    allConfigs.add(APGAS_BACKUPCOUNT);
    allConfigs.add(APGAS_PPERNODE);
    allConfigs.add(APGAS_MASTER);
    allConfigs.add(APGAS_HOSTFILE);
    allConfigs.add(APGAS_JAVA);
    allConfigs.add(APGAS_VERBOSE_LAUNCHER);
    allConfigs.add(APGAS_LAUNCHER);
    allConfigs.add(APGAS_PLACE_ID);
    allConfigs.add(APGAS_PORT);
    allConfigs.add(APGAS_NETWORK_INTERFACE);
    allConfigs.add(APGAS_CONSOLEPRINTER);
    allConfigs.add(APGAS_HOSTMANAGER_STRATEGY);
    allConfigs.add(APGAS_ELASTIC);
    allConfigs.add(APGAS_MALLEABLE_COMMUNICATOR);
    
    for (final Configuration<?> c : allConfigs) {
      c.get();
    }
  }

  /**
   * retrieve the PropertyValue of the Configuration.
   *
   * <p>This returns the default value if provided and no other Value was set or retrieved via the
   * System-Properties. If a Value is set via the System-Properties this will override the default
   * Value. If a Value is set via the setter Method, this Value will override the default Value as
   * well as the System-Property Value.
   *
   * @return The Value of this Configuration
   */
  @SuppressWarnings("unchecked")
  public synchronized T get() {

    if (this.cachedValue != null) {
      return cachedValue;
    }

    String value = System.getProperty(this.name);
    if (value == null) {
      if (this.defaultValue != null) {
        this.set(this.defaultValue);
      }
      return defaultValue;
    }

    if (this.propertyType.equals(Boolean.class)) {
      Boolean aBoolean = Boolean.valueOf(value);
      this.cachedValue = (T) aBoolean;
      return this.cachedValue;
    }

    if (this.propertyType.equals(Integer.class)) {
      Integer anInt = Integer.valueOf(value);
      this.cachedValue = (T) anInt;
      return this.cachedValue;
    }

    if (this.propertyType.equals(Double.class)) {
      Double aDouble = Double.valueOf(value);
      this.cachedValue = (T) aDouble;
      return this.cachedValue;
    }

    if (this.propertyType.equals(String.class)) {
      this.cachedValue = (T) value;
      return this.cachedValue;
    }

    return (T) value;
  }

  /**
   * sets the default value to use if no System-Property is present. This can be overridden by a set
   * call.
   *
   * @param defaultValue The Value to use as default
   */
  public synchronized void setDefaultValue(T defaultValue) {
    this.defaultValue = defaultValue;
  }

  /**
   * set the given Value as Value for this Configuration.
   *
   * @param value The Value to Set for this Configuration
   */
  public synchronized void set(T value) {
    this.cachedValue = value;
    System.setProperty(this.name, String.valueOf(this.cachedValue));
  }

  /**
   * getter
   *
   * @return The Name of the Configuration
   */
  public synchronized String getName() {
    return name;
  }
}
