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

package apgas.impl;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;

import apgas.Configuration;
import apgas.Constructs;
import apgas.GlobalRuntime;
import apgas.MultipleException;
import apgas.Place;
import apgas.SerializableCallable;
import apgas.SerializableJob;
import apgas.impl.Finish.Factory;
import apgas.impl.elastic.MalleableCommunicator;
import apgas.impl.elastic.MalleableHandler;
import apgas.launcher.Launcher;
import apgas.launcher.SshLauncher;
import apgas.util.GlobalID;
import apgas.util.GlobalRef;
import apgas.util.MyForkJoinPool;

/**
 * The {@link GlobalRuntimeImpl} class implements the {@link GlobalRuntime}
 * class.
 */
public final class GlobalRuntimeImpl extends GlobalRuntime {

	/**
	 * Synchronization Object for the Malleability Methods: AddPlaces and
	 * RemovePlaces
	 */
	static final Object MALLEABILITY_SYNC = new Object();

	private static GlobalRuntimeImpl runtime;

	private static Worker currentWorker() {
		final Thread t = Thread.currentThread();
		return t instanceof Worker ? (Worker) t : null;
	}

	/**
	 * Obtain the current runtime implementation
	 *
	 * @return {@link GlobalRuntimeImpl} object of the current runtime
	 */
	public static GlobalRuntimeImpl getRuntime() {
		while (!runtime.ready) { // Wait for constructor
			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
			}
		}
		return runtime;
	}

	private static boolean isReachable(NetworkInterface nif, String addr, int openPort, int timeOutMillis) {
		final Enumeration<InetAddress> nifAddresses = nif.getInetAddresses();
		if (!nifAddresses.hasMoreElements()) {
			return false;
		}
		try {
			try (Socket soc = new Socket()) {
				final InetAddress inetAddress = nifAddresses.nextElement();
				if (inetAddress == null) {
					return false;
				}
				soc.bind(new InetSocketAddress(inetAddress, 0));
				soc.connect(new InetSocketAddress(addr, openPort), timeOutMillis);
			}
			return true;
		} catch (final IOException ex) {
			return false;
		}
	}

	/** True if shutdown is in progress. */
	private boolean dying;
	/** The time of the last place failure. */
	Long failureTime;
	/** The finish factory. */
	private final Factory finishFactory;
	/** The registered place failure handler. */
	private Consumer<Place> handler;
	/** This place's ID. */
	final int here;
	/** This place. */
	private final Place home;
	/** Only used on place 0, manages the relation between hosts and places */
	private final HostManager hostManager;

	/** A extra pool for immediate calls. */
	final ThreadPoolExecutor immediatePool;
	/** The initial number of places */
	final int initialPlaces;
	/** Flag that indicates that this Place is the Master */
	private final boolean isMaster;
	/** The launcher used to spawn additional places. */
	private Launcher launcher;
	/** Address of this host */
	private final String localhost;
	/**
	 * The unit in charge of communicating with the scheduler to receive incoming
	 * malleable requests
	 */
	public transient MalleableCommunicator malleableCommunicator;
	/**
	 * The handler set by the user in charge of informing the running program of
	 * incoming changes
	 */
	public transient MalleableHandler malleableHandler;
	/** An immutable ordered list of the current places. */
	private List<Place> places;
	/** The mutable set of places in this global runtime instance. */
	private final SortedSet<Place> placeSet = new TreeSet<>();
	/** The pool for this global runtime instance. */
	final MyForkJoinPool pool;
	/** Indicates if the instance is ready */
	public final boolean ready;
	/** The value of the APGAS_RESILIENT system property. */
	private final boolean resilient;
	/** The resilient map from finish IDs to finish states. */
	final IMap<GlobalID, ResilientFinishState> resilientFinishMap;

	/** The registered runtime failure handler. */
	Runnable shutdownHandler;

	/** timeout for apgas runtime starting in seconds */
	int timeoutStarting = 500;

	/** The transport for this global runtime instance. */
	final Transport transport;

	/** Verbose of the Launcher */
	final boolean verboseLauncher;

	/**
	 * Constructs a new {@link GlobalRuntimeImpl} instance.
	 *
	 * @param args the command line arguments
	 */
	public GlobalRuntimeImpl(String[] args) {
		final long begin = System.nanoTime();
		GlobalRuntimeImpl.runtime = this;

		// parse configuration
		localhost = InetAddress.getLoopbackAddress().getHostAddress();
		isMaster = Configuration.CONFIG_APGAS_MASTER.get() == null;
		final String master = isMaster ? null : Configuration.CONFIG_APGAS_MASTER.get();
		final List<String> hostNames = readHostfile();
		if (isMaster) {
			hostManager = new HostManager(hostNames, localhost);
		} else {
			hostManager = null;
		}
		Configuration.initAll();
		initialPlaces = Configuration.CONFIG_APGAS_PLACES.get();
		verboseLauncher = Configuration.CONFIG_APGAS_VERBOSE_LAUNCHER.get();
		final String elasticityMode = Configuration.CONFIG_APGAS_ELASTIC.get();
		if (elasticityMode.equals("malleable")) {
			resilient = true;
		} else {
			resilient = Configuration.CONFIG_APGAS_RESILIENT.get();
		}
		final int maxThreads = Configuration.CONFIG_APGAS_MAX_THREADS.get();
		final int backupCount = Configuration.CONFIG_APGAS_BACKUPCOUNT.get();
		final int placeID = Configuration.CONFIG_APGAS_PLACE_ID.get();

		if (verboseLauncher) {
			System.err.println("JVM of Place " + placeID + " started");
		}

		final String ip = selectGoodIPForHost();

		if (isMaster) {
			initializeLauncher();
			GlobalRuntime.readyCounter = new AtomicInteger(initialPlaces);
		}

		finishFactory = resilient ? new ResilientFinishOpt.Factory() : new DefaultFinish.Factory();

		// initialize scheduler
		pool = new MyForkJoinPool(Configuration.CONFIG_APGAS_THREADS.get(), maxThreads, new WorkerFactory(), null);

		immediatePool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Configuration.CONFIG_APGAS_IMMEDIATE_THREADS.get());

		// Initialize transport
		transport = new Transport(this, master, ip, backupCount, placeID);
		transport.startHazelcast();

		// If this is the master, launch the other processes
		if (isMaster) {
			launchPlaces(master, ip);
		}
		waitForAllHazelcastMembers();

		here = transport.here();
		home = new Place(here);
		resilientFinishMap = resilient ? transport.getResilientFinishMap() : null;

		if (verboseLauncher) {
			System.err.println("[APGAS] New place was started at " + transport.getAddress());
		}

		if (isMaster) {
			installThreads(master);
		}

		// Initialize elastic communicator if need be
		if (elasticityMode.equals(Configuration.APGAS_ELASTIC_MALLEABLE) && isMaster) {
			// The runtime is malleable, need to initialize the malleable communicator
			final String communicatorClassName = Configuration.CONFIG_APGAS_MALLEABLE_COMMUNICATOR.get();
			try {
				malleableCommunicator = (MalleableCommunicator) Class.forName(communicatorClassName)
						.getDeclaredConstructor().newInstance();
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException
					| ClassNotFoundException e) {
				System.err.println("Something went wrong when trying to instanciate malleable communicator "
						+ communicatorClassName);
				e.printStackTrace();
				System.err.println("The specified class should have a public constructor with no parameters.");
			}
			if (verboseLauncher) {
				System.err.println(
						"Initialized Malleable Communicator " + malleableCommunicator.getClass().getCanonicalName());
			}
		}

		// start monitoring cluster
		transport.start();

		// wait for enough places to join the global runtime
		try {
			waitForAllPlaces();
		} catch (final Exception e) {
			e.printStackTrace();
		}

		ready = true;
		reduceReadyCounter();

		if (here == 0 && verboseLauncher) {
			System.out.println("[APGAS] Place startup time: " + ((System.nanoTime() - begin) / 1E9) + " sec");
		}
	}

	/**
	 * Submits a new task to the global runtime to be run at {@link Place} {@code p}
	 * with body {@code
	 * f} and returns immediately.
	 *
	 * @param p the place of execution
	 * @param f the function to run
	 */
	public void asyncAt(Place p, SerializableJob f) {
		final Worker worker = currentWorker();
		final Finish finish = worker == null || worker.task == null ? NullFinish.SINGLETON : worker.task.finish;
		finish.spawn(p.id);

		new Task(finish, f, here).asyncAt(p.id);
	}

	/**
	 * Submits a new local task to the global runtime with body {@code f} and
	 * returns immediately.
	 *
	 * @param f the function to run
	 */
	public void asyncFork(SerializableJob f) {
		final Worker worker = currentWorker();
		final Finish finish = worker == null || worker.task == null ? NullFinish.SINGLETON : worker.task.finish;

		finish.spawn(here);
		final Task task = new Task(finish, f, here);
		task.async(worker);
	}

	/**
	 * Evaluates {@code f} at {@link Place} {@code p}, waits for all the tasks
	 * transitively spawned by {@code f}, and returns the result.
	 *
	 * @param <T> the type of the result (must implement java.io.Serializable)
	 * @param p   the place of execution
	 * @param f   the function to run
	 * @return the result of the evaluation
	 */
	@SuppressWarnings("unchecked")
	public <T extends Serializable> T at(Place p, SerializableCallable<T> f) {
		final GlobalID id = new GlobalID();
		final Place _home = here();
		Constructs.finish(() -> Constructs.asyncAt(p, () -> {
			final T _result = f.call();
			Constructs.asyncAt(_home, () -> id.putHere(_result));
		}));
		return (T) id.removeHere();
	}

	/**
	 * Runs {@code f} at {@link Place} {@code p} and waits for all the tasks
	 * transitively spawned by {@code f}.
	 *
	 * <p>
	 * Equivalent to {@code finish(() -> asyncAt(p, f))}
	 *
	 * @param p the place of execution
	 * @param f the function to run
	 */
	public void at(Place p, SerializableJob f) {
		Constructs.finish(() -> Constructs.asyncAt(p, f));
	}

	/**
	 * Sub-routine called in case the malleable communicator fails for some reason.
	 */
	private void disableMalleableCommunicator() {
		// Diable the malleable communicator
		System.err.println("The Malleable Communicator sufferred an issue and will be stopped");
		try {
			malleableCommunicator.stop();
		} catch (final Exception e) {
			System.err.println("An error occurred while trying to shut down the malleable communicator");
			e.printStackTrace();
		} finally {
			malleableCommunicator = null;
			malleableHandler = null;
		}
	}

	/**
	 * Submits a task to the pool making sure that a thread will be available to run
	 * it.
	 *
	 * @param task the task
	 */
	void execute(ForkJoinTask<?> task) {
		pool.execute(task);
	}

	/**
	 * Submits a task to the the extra immediate pool.
	 *
	 * @param task the task
	 */
	void executeImmediate(Runnable task) {
		immediatePool.execute(task);
	}

	/**
	 * Evaluates {@code f}, waits for all the tasks transitively spawned by
	 * {@code f}, and returns the result.
	 *
	 * <p>
	 * If {@code f} or the tasks transitively spawned by {@code f} have uncaught
	 * exceptions then {@code finish(F)} then throws a {@link MultipleException}
	 * that collects these uncaught exceptions.
	 *
	 * @param <T> the type of the result
	 * @param f   the function to run
	 * @return the result of the evaluation
	 */
	public <T> T finish(Callable<T> f) {
		final Cell<T> cell = new Cell<>();
		finish(() -> cell.set(f.call()));
		return cell.get();
	}

	/**
	 * Runs {@code f} then waits for all tasks transitively spawned by {@code f} to
	 * complete.
	 *
	 * <p>
	 * If {@code f} or the tasks transitively spawned by {@code f} have uncaught
	 * exceptions then {@code finish(f)} then throws a {@link MultipleException}
	 * that collects these uncaught exceptions.
	 *
	 * @param f the function to run
	 * @throws MultipleException if there are uncaught exceptions
	 */
	public void finish(SerializableJob f) {
		final Worker worker = currentWorker();
		final Finish finish = finishFactory
				.make(worker == null || worker.task == null ? NullFinish.SINGLETON : worker.task.finish);
		new Task(finish, f, here).finish(worker);
		final List<Throwable> exceptions = finish.exceptions();
		if (exceptions != null) {
			throw MultipleException.make(exceptions);
		}
	}

	/**
	 * Returns the Worker running the current task
	 *
	 * @return Worker object of the current task
	 */
	public Worker getCurrentWorker() {
		return (Worker) Thread.currentThread();
	}

	@Override
	public ExecutorService getExecutorService() {
		return pool;
	}

	/**
	 * Returns the current {@link Place}.
	 *
	 * @return the current place
	 */
	public Place here() {
		return home;
	}

	/**
	 * /** Submits an immediate task to the global runtime to be run at
	 * {@link Place} {@code p} with body {@code f}.
	 *
	 * @param member the hazelcast member of execution
	 * @param f      the function to run
	 */
	public void immediateAsyncAt(Member member, SerializableRunnable f) {
		new ImmediateTask(f).immediateAsyncAt(member);
	}

	/**
	 * /** Submits an immediate task to the global runtime to be run at
	 * {@link Place} {@code p} with body {@code f}.
	 *
	 * @param p the place of execution
	 * @param f the function to run
	 */
	public void immediateAsyncAt(Place p, SerializableRunnable f) {
		new ImmediateTask(f).immediateAsyncAt(p.id);
	}

	/**
	 * initializes the Launcher to start new Places with.
	 */
	private void initializeLauncher() {
		final String launcherName = Configuration.CONFIG_APGAS_LAUNCHER.get();
		Launcher localLauncher = null;
		if (isMaster && launcherName != null) {
			try {
				localLauncher = (Launcher) Class.forName(launcherName).getDeclaredConstructor().newInstance();
			} catch (InstantiationException | IllegalAccessException | ExceptionInInitializerError
					| ClassNotFoundException | NoClassDefFoundError | ClassCastException | NoSuchMethodException
					| InvocationTargetException e) {
				System.err.println(
						"[APGAS] Unable to instantiate launcher: " + launcherName + ". Using default launcher (ssh).");
			}

			if (localLauncher == null) {
				localLauncher = new SshLauncher();
			}
			launcher = localLauncher;
		}
	}

	private void installThreads(String master) {
		// install hook on thread 1
		if (master == null) {
			final Thread[] thread = new Thread[Thread.activeCount()];
			Thread.enumerate(thread);
			for (final Thread t : thread) {
				if (t != null && t.getId() == 1) {
					new Thread(() -> {
						while (t.isAlive()) {
							try {
								t.join();
							} catch (final InterruptedException e) {
							}
						}
						shutdown();
					}).start();
					break;
				}
			}
		}
	}

	/**
	 * Returns the liveness of a place
	 *
	 * @param place the place whose state is to be checked
	 * @return isDead
	 */
	public boolean isDead(Place place) {
		return !places.contains(place);
	}

	@Override
	public Long lastfailureTime() {
		return failureTime;
	}

	private void launchPlaces(String master, String ip) {
		if (master == null) {
			try {
				hostManager.buildLaunchCommand(ip, getClass().getSuperclass().getCanonicalName());
				if (initialPlaces == 1) {
					return;
				}
				// launch additional places
				launcher.launch(hostManager, initialPlaces - 1, verboseLauncher);
			} catch (final Exception t) {
				// initiate shutdown
				t.printStackTrace();
				shutdown();
			}
		}
	}

	/**
	 * Returns the first unused place ID.
	 *
	 * @return the first unused place ID
	 */
	public int maxPlace() {
		return transport.maxPlace();
	}

	/**
	 * Returns the next place
	 *
	 * @param place the place whose successor is to be returned
	 * @return the next place
	 */
	public Place nextPlace(Place place) {
		final List<? extends Place> tmpPlaces = new ArrayList<>(places());
		for (final Place p : tmpPlaces) {
			if (p.id > place.id) {
				return p;
			}
		}
		return tmpPlaces.get(0);
	}

	/**
	 * Returns the place with the given ID.
	 *
	 * @param id the requested ID
	 * @return the place with the given ID
	 */
	public Place place(int id) {
		return new Place(id);
	}

	/**
	 * Returns the current list of places in the global runtime.
	 *
	 * @return the current list of places in the global runtime
	 */
	public List<? extends Place> places() {
		return places;
	}

	/**
	 * Returns the previous place
	 *
	 * @param place the place whose predecessor should be returned
	 * @return the previous place
	 */
	public Place prevPlace(Place place) {
		final List<? extends Place> tmpPlaces = new ArrayList<>(places());
		Place prev = tmpPlaces.get(tmpPlaces.size() - 1);
		for (final Place p : tmpPlaces) {
			if (p.id >= place.id) {
				return prev;
			}
			prev = p;
		}
		return prev;
	}

	/**
	 * reads in the Hosts from the given Filename in Configuration.APGAS_HOSTFILE.
	 * This file is loaded and the Hosts are extracted.
	 *
	 * @return The List of Hosts read in.
	 */
	private List<String> readHostfile() {
		final String hostfile = Configuration.CONFIG_APGAS_HOSTFILE.get();
		final List<String> hosts = new ArrayList<>();
		if (hostfile != null) {
			try {
				// hosts.addAll(Files.readAllLines(Paths.get("/home/kanzaki/posner-evolving-glb/hostfile")));
				hosts.addAll(Files.readAllLines(FileSystems.getDefault().getPath(hostfile)));
				if (hosts.isEmpty()) {
					System.err.println("[APGAS] Empty hostfile: " + hostfile + ". Using localhost.");
				}
			} catch (final IOException e) {
				System.err.println("[APGAS] Unable to read hostfile: " + hostfile + ". Using localhost.");
			}
		}
		return hosts;
	}

	private void reduceReadyCounter() {
		if (here != 0) {
			if (verboseLauncher) {
				System.err.println("[APGAS] " + here + " sends ready to place 0");
			}
			final int _h = here;
			try {
				final boolean _verboseLauncher = verboseLauncher;
				transport.send(0, new UncountedTask(() -> {
					final int value = GlobalRuntime.readyCounter.decrementAndGet();
					if (_verboseLauncher) {
						System.err.println("[APGAS] " + _h + " on place 0 decremented ready counter, is now " + value);
					}
				}));
			} catch (final Throwable e) {
				e.printStackTrace();
			}
			// place 0 && master
		} else {
			GlobalRuntime.readyCounter.decrementAndGet();

			while (GlobalRuntime.readyCounter.get() > 0) {
				if (verboseLauncher) {
					System.err.println("[APGAS] " + here + " not all constructors are ready, waiting...."
							+ GlobalRuntime.readyCounter.get());
				}
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (final InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * select a suitable IP for this Host.
	 *
	 * @return The suitable Host of this Place
	 */
	private String selectGoodIPForHost() {
		final String networkInterface = Configuration.CONFIG_APGAS_NETWORK_INTERFACE.get();
		final String cleanNetworkInterface;
		if (networkInterface != null && networkInterface.length() > 0) {
			cleanNetworkInterface = networkInterface.replaceAll(".\\*", "");
		} else {
			cleanNetworkInterface = "";
		}

		String ip = null;
		String master = Configuration.CONFIG_APGAS_MASTER.get();
		if (master == null && hostManager != null) {
			for (final String h : hostManager.getHostNames()) {
				try {
					if (!InetAddress.getByName(h).isLoopbackAddress()) {
						master = h;
						break;
					}
				} catch (final UnknownHostException ignored) {
				}
			}
		}

		if (master == null) {
			master = localhost;
		}

		try {
			final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

			while (networkInterfaces.hasMoreElements()) {
				final NetworkInterface ni = networkInterfaces.nextElement();

				try {
					// if (!InetAddress.getByName(master.split(":")[0]).isReachable(ni, 0, 100)) {
					if (isReachable(ni, master.split(":")[0], 5701, 100)) {
						if (verboseLauncher) {
							System.err.println(
									"[APGAS] host " + master + " is not reachable with networkinterface " + ni);
						}
						continue;
					}

				} catch (final Throwable t) {
					if (verboseLauncher) {
						System.err.println("[APGAS] unexpected error in finding host");
					}
					t.printStackTrace();
				}
				if (verboseLauncher) {
					System.err.println("[APGAS] host " + master + " is reachable with network interface " + ni);
				}
				final Enumeration<InetAddress> e = ni.getInetAddresses();
				while (e.hasMoreElements()) {
					final InetAddress inetAddress = e.nextElement();
					// force apgas.network.interface
					if (inetAddress.isLoopbackAddress() || inetAddress instanceof Inet6Address || (cleanNetworkInterface.length() > 0
							&& !inetAddress.getHostAddress().contains(cleanNetworkInterface))) {
						continue;
					}

					ip = inetAddress.getHostAddress();
				}
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}

		if (verboseLauncher) {
			System.err.println("[APGAS] found ip: " + (null == ip ? "null" : ip));
		}
		return ip;
	}

	/**
	 * Used to define the handler for malleable programs
	 *
	 * @param handler the handler to use from now on.
	 */
	public synchronized void setMalleableHandler(MalleableHandler handler) {
		if (verboseLauncher) {
			System.err.println("Setting malleable handler " + handler.getClass().getCanonicalName());
		}
		if (here != 0) {
			throw new RuntimeException("Attempted to set the malleable handler on place(" + here
					+ "). The malleable handler should only be set on place(0)");
		}
		if (malleableCommunicator == null) {
			// Either initialization failed or you forgot to set and this program
			// "malleable".
			// Either way this program is effectively fixed, making defining the malleable
			// handler redundant"
			throw new RuntimeException("The malleable communicator was not instanciated");
		}
		if (malleableHandler != null) {
			throw new RuntimeException("The malleable handler is already set, ignoring");
		}
		malleableHandler = handler;

		// Starting the communicator
		try {
			malleableCommunicator.start();
		} catch (final Exception e) {
			e.printStackTrace();
			disableMalleableCommunicator();
		}
	}

	@Override
	public void setPlaceFailureHandler(Consumer<Place> handler) {
		this.handler = handler;
	}

	@Override
	public void setRuntimeShutdownHandler(Runnable handler) {
		shutdownHandler = handler;
	}

	@Override
	public void shutdown() {
		synchronized (this) {
			if (dying) {
				return;
			}
			dying = true;
		}
		// Shutdown was decided. Before anything else, we stop receiving grow/shrink
		// orders from the scheduler (if running in malleable mode)
		if (malleableCommunicator != null) {
			malleableCommunicator.stop();
		}

		// Run the shutdown handler if it was defined
		if (shutdownHandler != null) {
			shutdownHandler.run();
		}

		// Run the launcher-defined shutdown handler
		if (launcher != null) {
			launcher.shutdown();
		}

		// Turn off the worker pool to stop running asynchronous tasks
		pool.shutdown();
		immediatePool.shutdown();
		// Turn off the communication layer with the other processes
		transport.shutdown();

		// Exit
		if (verboseLauncher) {
			System.err.println("place(" + here + ") is shutting down.");
		}
		System.exit(0);
	}

	/**
	 * Sub routine used to launch the shutdown of the places given as parameter.
	 *
	 * @param toBeRemoved list of places to release
	 * @return the name of the hosts of freed processes in a list
	 */
	private List<String> shutdownMallPlaces(final List<Place> toBeRemoved) {
		if (!isMaster) {
			System.err.println(
					"[APGAS] " + home + " called shutdownMallPlaces(), but only the master is allowed to do this");
			return null;
		}
		if (verboseLauncher) {
			System.err.println("[APGAS] shuting down " + toBeRemoved);
		}
		final ArrayList<String> removedHosts = new ArrayList<>();

		synchronized (MALLEABILITY_SYNC) {
			for (final Place placeToBeRemoved : toBeRemoved) {
				final int placeIdToRemove = placeToBeRemoved.id;
				if (here == placeIdToRemove) {
					System.err.println("[APGAS] cannot remove myself: " + toBeRemoved);
					toBeRemoved.remove(placeIdToRemove);
					continue;
				}

				final Member memberToRemove = transport.getMembers().get(placeIdToRemove);

				removedHosts.add(hostManager.detachFromHost(placeToBeRemoved));
				if (verboseLauncher) {
					System.err.println(hostManager);
				}

				// the other remaining places are automatically refreshed by
				// Transport:memberRemoved
				new ImmediateTask(() -> {
					// should be wait for finishing all tasks on the host shutting down
					GlobalRuntimeImpl.getRuntime().shutdown();
				}).immediateAsyncAt(memberToRemove);
			}
		}
		return removedHosts;
	}

	/**
	 * Procedure used to release the places given as parameter. This procedure is
	 * called by the {@link MalleableCommunicator} after calling the pre-hook
	 * provided by the programmer
	 *
	 * @param toRelease list of places to release
	 * @return the name of the hosts of freed processes in a list
	 */
	public List<String> shutdownMallPlacesBlocking(List<Place> toRelease) {
		List<String> freedHosts = null;
		synchronized (MALLEABILITY_SYNC) {
			final int initialPlacesCount = places.size();
			final int expectedPlacesCount = initialPlacesCount - toRelease.size();
			freedHosts = shutdownMallPlaces(toRelease);

			// Wait on place 0 for the number of places to reach the expected level
			waitForNewPlacesCount(expectedPlacesCount);

			// Make sure all the other places are also informed of the change in the number
			// of processes
			final GlobalRef<CountDownLatch> globalRef = new GlobalRef<>(new CountDownLatch(expectedPlacesCount - 1));
			for (final Place p : places()) {
				if ((p.id == here().id) || toRelease.contains(p)) {
					continue;
				}
				final boolean verbose = verboseLauncher;
				immediateAsyncAt(p, () -> {
					if (verbose) {
						System.err.println(p + " was informed of the reduction in the number of hosts");
					}
					GlobalRuntimeImpl.getRuntime().waitForNewPlacesCount(expectedPlacesCount);
					GlobalRuntimeImpl.getRuntime().immediateAsyncAt(globalRef.home(), () -> {
						globalRef.get().countDown();
					});
				});
			}
			try {
				globalRef.get().await();
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
		}
		return freedHosts;
	}

	/**
	 * Asynchronously start new places
	 *
	 * @param n number of places to spawn
	 * @return a future returning a list of integers containing the place ids of the
	 *         new places
	 */
	private Future<List<Integer>> startMallPlaces(int n) {
		if (!isMaster) {
			System.err.println(
					"[APGAS] " + home + " called startMallPlaces(), but only the master is allowed to do this");
			return null;
		}

		synchronized (MALLEABILITY_SYNC) {
			//  not needed here, only for cosmetic
			GlobalRuntime.readyCounter.addAndGet(n);
			try {
				ExecutorService executor = Executors.newSingleThreadExecutor();
				return executor.submit(() -> launcher.launch(hostManager, n, verboseLauncher));
			} catch (final Exception e) {
				e.printStackTrace();
				return null;
			}
		}
	}

	/**
	 * Method called to add places to the program currently in execution
	 *
	 * @param n     number of new places to spawn
	 * @param hosts hosts on which to spawn these new places
	 * @return list of integers containing the ids of the newly spawned places
	 */
	public List<Integer> startMallPlacesBlocking(int n, List<String> hosts) {
		for (final String host : hosts) {
			hostManager.addHost(host);
		}
		synchronized (MALLEABILITY_SYNC) {
			final int initialPlacesCount = places.size();
			final int expectedPlacesCount = initialPlacesCount + n;
			final Future<List<Integer>> listFuture = startMallPlaces(n);
			// wait on place 0
			waitForNewPlacesCount(expectedPlacesCount);

			// wait on all other places
			final GlobalRef<CountDownLatch> globalRef = new GlobalRef<>(new CountDownLatch(places().size() - 1));
			for (final Place p : places()) {
				if (p.id == here().id) {
					continue;
				}
				immediateAsyncAt(p, () -> {
					GlobalRuntimeImpl.getRuntime().waitForNewPlacesCount(expectedPlacesCount);
					GlobalRuntimeImpl.getRuntime().immediateAsyncAt(globalRef.home(), () -> {
						globalRef.get().countDown();
					});
				});
			}
			try {
				globalRef.get().await();
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}

			if (listFuture != null) {
				try {
					return listFuture.get();
				} catch (final InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
			return Collections.emptyList();
		}
	}

	/**
	 * Submits an uncounted task to the global runtime to be run at {@link Place}
	 * {@code p} with body {@code f} and returns immediately. The termination of
	 * this task is not tracked by the enclosing finish. Exceptions thrown by the
	 * task are ignored.
	 *
	 * @param p the place of execution
	 * @param f the function to run
	 */
	public void uncountedAsyncAt(Place p, SerializableJob f) {
		new UncountedTask(f).uncountedAsyncAt(p.id);
	}

	/**
	 * Updates the place collections.
	 *
	 * @param added   added places
	 * @param removed removed places
	 */
	public void updatePlaces(List<Integer> added, List<Integer> removed) {
		synchronized (placeSet) {
			for (final int id : added) {
				placeSet.add(new Place(id));
			}
			for (final int id : removed) {
				placeSet.remove(new Place(id));
			}
			places = Collections.unmodifiableList(new ArrayList<>(placeSet));
		}
		if (removed.isEmpty()) {
			return;
		}
		if (!resilient) {
			shutdown();
			return;
		}
		final Consumer<Place> localHandler = handler;
		execute(new RecursiveAction() {
			private static final long serialVersionUID = 1052937749744648347L;

			@Override
			public void compute() {
				final Worker worker = (Worker) Thread.currentThread();
				worker.task = null; // a handler is not a task (yet)
				for (final int id : removed) {
					ResilientFinishState.purge(id);
				}
				if (localHandler != null) {
					for (final int id : removed) {
						localHandler.accept(new Place(id));
					}
				}
			}
		});
	}

	private void waitForAllHazelcastMembers() {
		final long beforeStart = System.nanoTime();
		while (transport.hazelcast.getCluster().getMembers().size() < initialPlaces) {
			final long now = System.nanoTime();
			if ((now - beforeStart) / 1E9 > timeoutStarting) {
				System.err.println(
						"[APGAS] " + ManagementFactory.getRuntimeMXBean().getName() + " ran into timeout, exit JVM");
				System.exit(40);
			}

			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (final InterruptedException interruptedException) {
				interruptedException.printStackTrace();
			}
		}

		if (verboseLauncher) {
			System.err.println(ManagementFactory.getRuntimeMXBean().getName() + " all hazelcast members are connected");
		}
	}

	private void waitForAllPlaces() throws Exception {
		final long beforeMaxPlaces = System.nanoTime();
		while (places.size() < initialPlaces) {
			try {
				TimeUnit.SECONDS.sleep(1);
				if (verboseLauncher) {
					System.err.println("[APGAS] " + ManagementFactory.getRuntimeMXBean().getName() + ", " + home
							+ ": not all places are started, places.size()= " + places.size() + ", initialPlaces="
							+ initialPlaces);
				}

				final long now = System.nanoTime();
				if ((now - beforeMaxPlaces) / 1E9 > timeoutStarting) {
					System.err.println("[APGAS] " + ManagementFactory.getRuntimeMXBean().getName()
							+ " ran into timeout, exit JVM");
					System.exit(40);
				}
			} catch (final InterruptedException e) {
			}
			if (launcher != null && !launcher.healthy()) {
				throw new Exception("A process exited prematurely");
			}
		}

		if (verboseLauncher) {
			System.err.println("[APGAS] " + ManagementFactory.getRuntimeMXBean().getName() + ", " + home
					+ ": all places are started, places.size()= " + places.size() + ", initialPlaces=" + initialPlaces);
		}
	}

	private void waitForNewPlacesCount(int expectedPlacesCount) {
		while ((places().size() != expectedPlacesCount)
				|| (transport.hazelcast.getCluster().getMembers().size() != expectedPlacesCount)
				|| (!transport.hazelcast.getPartitionService().isClusterSafe())
				|| (transport.getMembers().values().size() != expectedPlacesCount)) {
			if (verboseLauncher) {
				System.out.println("[APGAS] Place(" + here + "): waiting for malleability, places().size()="
						+ places().size() + ", expectedPlacesCount=" + expectedPlacesCount);
			}
			try {
				TimeUnit.MILLISECONDS.sleep(500);
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
