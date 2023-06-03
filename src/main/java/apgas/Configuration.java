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

import java.util.ArrayList;
import java.util.List;

import apgas.impl.elastic.SocketMalleableCommunicator;

/**
 * The {@link Configuration} class defines the names of the system properties
 * used to configure the global runtime.
 *
 * <p>
 * This Class provides only String, Integer, Boolean and Double values at the
 * moment. If you need more, you have to add them manually. In
 * {@code Configuration.get()}.
 */
public final class Configuration<T> {

	/**
	 * Count of backups used by the internal distributed data memory. (Integer
	 * property)
	 *
	 * <p>
	 * Set to value between {@code 0} (inclusive) and {@code 6} (inclusive).
	 * Defaults to "{@code
	 * 1}".
	 */
	public static final String APGAS_BACKUPCOUNT_PROPERTY = "apgas.backupcount";

	/** This Property enables and disables the consoleprinter. */
	public static final String APGAS_CONSOLEPRINTER_PROPERTY = "apgas.consoleprinter";

	/**
	 * Possible value for configuration {@link #APGAS_ELASTIC_PROPERTY}.
	 */
	public static final String APGAS_ELASTIC_FIXED = "fixed";

	/**
	 * Possible value for configuration {@link #APGAS_ELASTIC_PROPERTY}. This value
	 * sets the runtime to be malleable. A malleable communicator will be prepared
	 * by the runtime to receive instructions from the scheduler. The programmer
	 * should call method
	 * {@link Constructs#defineMalleableHandle(apgas.impl.elastic.MalleableHandler)}
	 * to define the actions to perform before and after a malleable change.
	 */
	public static final String APGAS_ELASTIC_MALLEABLE = "malleable";

	/**
	 * This String is used to define the property setting which indicated if the
	 * running APGAS program is elastic. Possible values are:
	 * <ul>
	 * <li>fixed: (default), no change in the number of places during execution
	 * <li>evolving: (Not implemented yet), the running program may initiate
	 * requests for additional places / release some during execution
	 * <li>malleable: the running program may dynamically change the number of
	 * running places following orders given by the scheduler
	 * </ul>
	 */
	public static final String APGAS_ELASTIC_PROPERTY = "apgas.elastic";

	/**
	 * Property {@value #APGAS_HOSTFILE_PROPERTY} specifies a filename that lists
	 * hosts on which to launch places (String property).
	 */
	public static final String APGAS_HOSTFILE_PROPERTY = "apgas.hostfile";
	/**
	 * This Property defines the strategy of the hostmanager:
	 * <ul>
	 * <li>0 : Places are added cyclical to the nodes (default)
	 * <li>1 : Nodes are filled one after the other. This can result in hosts
	 * without places
	 * </ul>
	 */
	public static final String APGAS_HOSTMANAGER_STRATEGY_PROPERTY = "apgas.hostmanager.strategy";

	/**
	 * Property {@value #APGAS_IMMEDIATE_THREADS_PROPERTY} specifies the desired
	 * level of parallelism for the extra immediate thread pool (Integer property).
	 *
	 * <p>
	 * 4 is used by default
	 */
	public static final String APGAS_IMMEDIATE_THREADS_PROPERTY = "apgas.immediate.threads";

	/**
	 * Specifies the java command to run for spawning places (String property).
	 *
	 * <p>
	 * Defaults to "{@code java}".
	 */
	public static final String APGAS_JAVA_PROPERTY = "apgas.java";

	/**
	 * Name of the launcher implementation class to instantiate (String property).
	 *
	 * <p>
	 * Defaults to "{@code apgas.launcher.SshLauncher}".
	 */
	public static final String APGAS_LAUNCHER_PROPERTY = "apgas.launcher";

	/**
	 * String used to define the class used as malleable communicator
	 */
	public static final String APGAS_MALLEABLE_COMMUNICATOR_PROPERTY = "apgas.malleable_communicator";

	/**
	 * Property {@value #APGAS_MASTER_PROPERTY} optionally specifies the ip or
	 * socket address of the master node (String property). Normally, this property
	 * is set by the runtime automatically.
	 * <p>
	 * If set to an ip, the global runtime connects to the first available global
	 * runtime instance at this ip within the default port range.
	 */
	public static final String APGAS_MASTER_PROPERTY = "apgas.master";
	/**
	 * Upper bound on the number of persistent threads in the thread pool (Integer
	 * property).
	 *
	 * <p>
	 * Defaults to 256.
	 */
	public static final String APGAS_MAX_THREADS_PROPERTY = "apgas.max.threads";

	/**
	 * This Property can define the network interface to be used Examples: Kassel
	 * FB16: eth0: "141.51.205.*" eth1: "192.168.205.*" ib0: "192.168.169.*"
	 *
	 * <p>
	 * Goethe: eno1: "10.151.*.*" ib0: "10.149.*.*"
	 */
	public static final String APGAS_NETWORK_INTERFACE_PROPERTY = "apgas.network.interface";

	/**
	 * This Property is set by the Master or starting Place to set the id of the
	 * Place to start, at launch time. This gives the starting Place (Master atm.)
	 * the responsibility of the ID Management.
	 */
	public static final String APGAS_PLACE_ID_PROPERTY = "apgas.place.id";

	/**
	 * Property {@value #APGAS_PLACES_PROPERTY} specifies the desired number of
	 * places (Integer property).
	 *
	 * <p>
	 * Defaults to 1. If {@value #APGAS_PLACES_PROPERTY} is not set the global
	 * runtime spawns the desired number of places, otherwise it waits for the
	 * places to appear.
	 */
	public static final String APGAS_PLACES_PROPERTY = "apgas.places";

	/** This Property sets the Port of the Master Place. */
	public static final String APGAS_PORT_PROPERTY = "apgas.port";

	/**
	 * Places per Node
	 *
	 * <p>
	 * Defaults to 0 (not working)
	 */
	public static final String APGAS_PPERNODE_PROPERTY = "apgas.ppernode";

	/**
	 * Property {@value #APGAS_RESILIENT_PROPERTY} enables fault tolerance (Boolean
	 * property).
	 *
	 * <p>
	 * If set, the global runtime does not abort the execution if a place fails.
	 */
	public static final String APGAS_RESILIENT_PROPERTY = "apgas.resilient";

	/**
	 * Property {@value #APGAS_THREADS_PROPERTY} specifies the desired level of
	 * parallelism (Integer property).
	 *
	 * <p>
	 * The return value of {@code Runtime.getRuntime().availableProcessors()} is
	 * used if this property is not set.
	 */
	public static final String APGAS_THREADS_PROPERTY = "apgas.threads";

	/**
	 * Controls the verbosity of the launcher (Boolean property).
	 *
	 * <p>
	 * Defaults to "{@code false}".
	 */
	public static final String APGAS_VERBOSE_LAUNCHER_PROPERTY = "apgas.verbose.launcher";

	/** Configuration object for {@link #APGAS_BACKUPCOUNT_PROPERTY} */
	public static final Configuration<Integer> CONFIG_APGAS_BACKUPCOUNT = new Configuration<>(
			APGAS_BACKUPCOUNT_PROPERTY, 1, Integer.class);

	/** Configuration object for {@link #APGAS_CONSOLEPRINTER_PROPERTY} */
	public static final Configuration<Boolean> CONFIG_APGAS_CONSOLEPRINTER = new Configuration<>(
			APGAS_CONSOLEPRINTER_PROPERTY, false, Boolean.class);

	/**
	 * Property defining if the program is allowed to change the number of running
	 * processes during execution.
	 */
	public static final Configuration<String> CONFIG_APGAS_ELASTIC = new Configuration<>(APGAS_ELASTIC_PROPERTY,
			APGAS_ELASTIC_FIXED, String.class);

	/** Configuration object for {@link #APGAS_HOSTFILE_PROPERTY} */
	public static final Configuration<String> CONFIG_APGAS_HOSTFILE = new Configuration<>(APGAS_HOSTFILE_PROPERTY,
			String.class);

	/** Configuration object for {@link #APGAS_HOSTMANAGER_STRATEGY_PROPERTY} */
	public static final Configuration<Integer> CONFIG_APGAS_HOSTMANAGER_STRATEGY = new Configuration<>(
			APGAS_HOSTMANAGER_STRATEGY_PROPERTY, 0, Integer.class);

	/** Configuration object for {@link #APGAS_IMMEDIATE_THREADS_PROPERTY} */
	public static final Configuration<Integer> CONFIG_APGAS_IMMEDIATE_THREADS = new Configuration<>(
			APGAS_IMMEDIATE_THREADS_PROPERTY, 4, Integer.class);

	/** Configuration for property {@value #APGAS_JAVA_PROPERTY} */
	public static final Configuration<String> CONFIG_APGAS_JAVA = new Configuration<>(APGAS_JAVA_PROPERTY, "java",
			String.class);

	/** Configuration for property {@value #APGAS_VERBOSE_LAUNCHER_PROPERTY} */
	public static final Configuration<String> CONFIG_APGAS_LAUNCHER = new Configuration<>(APGAS_LAUNCHER_PROPERTY,
			"apgas.launcher.SshLauncher", String.class);

	/**
	 * Property defining the class used to communicate with the scheduler when a
	 * malleable program is running
	 */
	public static final Configuration<String> CONFIG_APGAS_MALLEABLE_COMMUNICATOR = new Configuration<>(
			APGAS_MALLEABLE_COMMUNICATOR_PROPERTY, SocketMalleableCommunicator.class.getCanonicalName(), String.class);

	/** Configuration object for {@link #APGAS_MASTER_PROPERTY} */
	public static final Configuration<String> CONFIG_APGAS_MASTER = new Configuration<>(APGAS_MASTER_PROPERTY,
			String.class);

	/** Configuration for property {@value #APGAS_MAX_THREADS_PROPERTY} */
	public static final Configuration<Integer> CONFIG_APGAS_MAX_THREADS = new Configuration<>(
			APGAS_MAX_THREADS_PROPERTY, 256, Integer.class);

	/** Configuration for property {@value #APGAS_NETWORK_INTERFACE_PROPERTY} */
	public static final Configuration<String> CONFIG_APGAS_NETWORK_INTERFACE = new Configuration<>(
			APGAS_NETWORK_INTERFACE_PROPERTY, String.class);

	/** Configuration for property {@value #APGAS_PLACE_ID_PROPERTY} */
	public static final Configuration<Integer> CONFIG_APGAS_PLACE_ID = new Configuration<>(APGAS_PLACE_ID_PROPERTY, 0,
			Integer.class);

	/** Configuration object for {@link #APGAS_PLACES_PROPERTY} */
	public static final Configuration<Integer> CONFIG_APGAS_PLACES = new Configuration<>(APGAS_PLACES_PROPERTY, 1,
			Integer.class);

	/** Configuration for property {@value #APGAS_PORT_PROPERTY} */
	public static final Configuration<Integer> CONFIG_APGAS_PORT = new Configuration<>(APGAS_PORT_PROPERTY, 5701,
			Integer.class);

	/** Configuration object for {@link #APGAS_RESILIENT_PROPERTY} */
	public static final Configuration<Boolean> CONFIG_APGAS_RESILIENT = new Configuration<>(APGAS_RESILIENT_PROPERTY,
			false, Boolean.class);

	/** Configuration object for {@link #APGAS_THREADS_PROPERTY} */
	public static final Configuration<Integer> CONFIG_APGAS_THREADS = new Configuration<>(APGAS_THREADS_PROPERTY,
			Runtime.getRuntime().availableProcessors(), Integer.class);

	/** Configuration for property {@value #APGAS_VERBOSE_LAUNCHER_PROPERTY} */
	public static final Configuration<Boolean> CONFIG_APGAS_VERBOSE_LAUNCHER = new Configuration<>(
			APGAS_VERBOSE_LAUNCHER_PROPERTY, false, Boolean.class);

	/**
	 * Initializes all the {@link Configuration} objects and reads the properties
	 * from the runtime to assign the adequate values to these objects
	 */
	public static void initAll() {
		final List<Configuration<?>> allConfigs = new ArrayList<>();
		allConfigs.add(CONFIG_APGAS_PLACES);
		allConfigs.add(CONFIG_APGAS_THREADS);
		allConfigs.add(CONFIG_APGAS_IMMEDIATE_THREADS);
		allConfigs.add(CONFIG_APGAS_MAX_THREADS);
		allConfigs.add(CONFIG_APGAS_RESILIENT);
		allConfigs.add(CONFIG_APGAS_BACKUPCOUNT);
		allConfigs.add(CONFIG_APGAS_MASTER);
		allConfigs.add(CONFIG_APGAS_HOSTFILE);
		allConfigs.add(CONFIG_APGAS_JAVA);
		allConfigs.add(CONFIG_APGAS_VERBOSE_LAUNCHER);
		allConfigs.add(CONFIG_APGAS_LAUNCHER);
		allConfigs.add(CONFIG_APGAS_PLACE_ID);
		allConfigs.add(CONFIG_APGAS_PORT);
		allConfigs.add(CONFIG_APGAS_NETWORK_INTERFACE);
		allConfigs.add(CONFIG_APGAS_CONSOLEPRINTER);
		allConfigs.add(CONFIG_APGAS_HOSTMANAGER_STRATEGY);
		allConfigs.add(CONFIG_APGAS_ELASTIC);
		allConfigs.add(CONFIG_APGAS_MALLEABLE_COMMUNICATOR);

		for (final Configuration<?> c : allConfigs) {
			c.get();
		}
	}

	/**
	 * Prints the configuration in use at every place of the running APGAS for Java
	 * runtime
	 */
	public static void printAllConfigs() {
		Constructs.finish(() -> {
			for (final Place place : Constructs.places()) {
				Constructs.at(place, () -> {
					Configuration.printConfigs();
				});
			}
		});
	}

	/**
	 * Prints all the various configuration items of the APGAS for Java runtime
	 */
	public static void printConfigs() {
		final List<Configuration<?>> allConfigs = new ArrayList<>();
		allConfigs.add(CONFIG_APGAS_PLACES);
		allConfigs.add(CONFIG_APGAS_THREADS);
		allConfigs.add(CONFIG_APGAS_IMMEDIATE_THREADS);
		allConfigs.add(CONFIG_APGAS_MAX_THREADS);
		allConfigs.add(CONFIG_APGAS_RESILIENT);
		allConfigs.add(CONFIG_APGAS_BACKUPCOUNT);
		allConfigs.add(CONFIG_APGAS_MASTER);
		allConfigs.add(CONFIG_APGAS_HOSTFILE);
		allConfigs.add(CONFIG_APGAS_JAVA);
		allConfigs.add(CONFIG_APGAS_VERBOSE_LAUNCHER);
		allConfigs.add(CONFIG_APGAS_LAUNCHER);
		allConfigs.add(CONFIG_APGAS_PLACE_ID);
		allConfigs.add(CONFIG_APGAS_PORT);
		allConfigs.add(CONFIG_APGAS_NETWORK_INTERFACE);
		allConfigs.add(CONFIG_APGAS_CONSOLEPRINTER);
		allConfigs.add(CONFIG_APGAS_HOSTMANAGER_STRATEGY);
		allConfigs.add(CONFIG_APGAS_ELASTIC);
		allConfigs.add(CONFIG_APGAS_MALLEABLE_COMMUNICATOR);

		final StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("APGAS config on " + Constructs.here() + ":\n");
		for (final Configuration<?> c : allConfigs) {
			stringBuilder.append("  " + c.getName() + "=" + c.get() + "\n");
		}
		System.out.println(stringBuilder.toString());
	}

	private T cachedValue;

	private T defaultValue;

	private final String name;

	private final Class<T> propertyType;

	/**
	 * Constructor
	 *
	 * @param name         The PropertyName of the Configuration Value
	 * @param propertyType The Type of the Property-Value
	 */
	private Configuration(final String name, final Class<T> propertyType) {
		this.name = name;
		this.propertyType = propertyType;
	}

	/**
	 * Constructor
	 *
	 * @param name         The PropertyName of the Configuration Value
	 * @param defaultValue A default Value to use if no one is provided via the
	 *                     System-Properties
	 * @param propertyType The Type of the Property-Value
	 */
	private Configuration(final String name, final T defaultValue, final Class<T> propertyType) {
		this.name = name;
		this.setDefaultValue(defaultValue);
		this.defaultValue = defaultValue;
		this.propertyType = propertyType;
	}

	/**
	 * retrieve the PropertyValue of the Configuration.
	 *
	 * <p>
	 * This returns the default value if provided, and no other Value was set or
	 * retrieved via the System-Properties. If a Value is set via the
	 * System-Properties this will override the default Value. If a Value is set via
	 * the setter Method, this Value will override the default Value as well as the
	 * System-Property Value.
	 *
	 * @return The Value of this Configuration
	 */
	@SuppressWarnings("unchecked")
	public synchronized T get() {

		if (cachedValue != null) {
			return cachedValue;
		}

		final String value = System.getProperty(name);
		if (value == null) {
			if (defaultValue != null) {
				this.set(defaultValue);
			}
			return defaultValue;
		}

		if (propertyType.equals(Boolean.class)) {
			final Boolean aBoolean = Boolean.valueOf(value);
			cachedValue = (T) aBoolean;
			return cachedValue;
		}

		if (propertyType.equals(Integer.class)) {
			final Integer anInt = Integer.valueOf(value);
			cachedValue = (T) anInt;
			return cachedValue;
		}

		if (propertyType.equals(Double.class)) {
			final Double aDouble = Double.valueOf(value);
			cachedValue = (T) aDouble;
			return cachedValue;
		}

		if (propertyType.equals(String.class)) {
			cachedValue = (T) value;
			return cachedValue;
		}

		return (T) value;
	}

	/**
	 * getter
	 *
	 * @return The Name of the Configuration
	 */
	public synchronized String getName() {
		return name;
	}

	/**
	 * set the given Value as Value for this Configuration.
	 *
	 * @param value The Value to Set for this Configuration
	 */
	public synchronized void set(T value) {
		cachedValue = value;
		System.setProperty(name, String.valueOf(cachedValue));
	}

	/**
	 * set the default value to use if no System-Property is present. This can be
	 * overridden by a set call.
	 *
	 * @param defaultValue The Value to use as default
	 */
	public synchronized void setDefaultValue(T defaultValue) {
		this.defaultValue = defaultValue;
	}
}
