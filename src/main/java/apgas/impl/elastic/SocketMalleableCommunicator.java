package apgas.impl.elastic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import apgas.Configuration;

/**
 * Default implementation of {@link MalleableCommunicator} based on a Socket.
 *
 * @author Patrick Finnerty
 *
 */
public class SocketMalleableCommunicator extends MalleableCommunicator {

	/**
	 * Setting to set the IP from which connections may be accepted to receive
	 * malleable orders
	 */
	public static final String SCHEDULER_IP = "malleable_scheduler_ip";
	/**
	 * Setting to set the port on which connections may be accepted to receive
	 * malleable orders
	 */
	public static final String SCHEDULER_Port = "malleable_scheduler_port";

	/** Thread in charge of (blockingly) waiting for incoming connections */
	private Thread listenerThread = null;
	/**
	 * Boolean flag used when shutting down the {@link #listenerThread} upon the
	 * program terminating
	 */
	private volatile boolean listening = true;

	/** Value of property {@link #SCHEDULER_IP} for this execution */
	private final String schedulerIP;
	/** Value of property {@link #SCHEDULER_Port} for this execution */
	private final int schedulerPort;

	/** Socket used to receive orders from the scheduler */
	private ServerSocket server = null;

	/**
	 * Socket of an ongoing connection
	 * <p>
	 * This socket is kept as a member for cases where as malleable "shrink" order
	 * is received and the hosts that were released need to be sent to the
	 * scheduler.
	 */
	private Socket socket = null;

	/**
	 * Flag used to set verbose mode. Is initialized based on the value set for
	 * property {@link Configuration#APGAS_VERBOSE_LAUNCHER}
	 */
	private final boolean verbose;

	/**
	 * Constructor
	 * <p>
	 * The SocketMalleableCommunicator users the properties {@link #SCHEDULER_IP}
	 * and {@link #SCHEDULER_Port} to initialize its socket. This constructor is
	 * called using reflection as part of a malleable APGAS execution.
	 *
	 * @throws Exception if thrown during the socket setup.
	 */
	public SocketMalleableCommunicator() throws Exception {
		// Obtain the IP/Port of the scheduler to establish a connection
		final Properties props = System.getProperties();
		if (!props.containsKey(SCHEDULER_IP) || !props.containsKey(SCHEDULER_Port)) {
			throw new Exception(
					"Cannot create the SocketMalleableCommunicator, either the IP or the port of the scheduler was not set");
		}
		schedulerIP = System.getProperty(SCHEDULER_IP);
		schedulerPort = Integer.parseInt(System.getProperty(SCHEDULER_Port));

		server = new ServerSocket();
		server.bind(new InetSocketAddress(schedulerIP, schedulerPort));

		verbose = Configuration.CONFIG_APGAS_VERBOSE_LAUNCHER.get();
	}

	@Override
	protected void hostReleased(List<String> hosts) {
		if (verbose) {
			for (final String s : hosts) {
				System.err.println("Released host: " + s);
			}
		}
		try {
			final PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
			for (final String hostName : hosts) {
				writer.println(hostName);
			}
		} catch (final Exception e) {
			System.err.println("Encountered issue while indicating the released hosts to the scheduler");
			e.printStackTrace();
		}
	}

	/**
	 * Procedure which blocks on the socket to receive orders from the scheduler.
	 * When a message is received, the thread running this procedure will call the
	 * appropriate superclass methods to trigger the change in the program runtime.
	 */
	private void listenToSocket() {
		while (listening) {
			try {
				socket = server.accept();
				final BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				final String line = reader.readLine();
				if (verbose) {
					System.out.println("SocketMalleableCommunicator received: " + line);
				}

				// Interpret received order and call the appropriate procedure
				final String str[] = line.split(" ");
				final String order = str[0];
				final int change = Integer.parseInt(str[1]);
				if (order.equals("shrink")) {
					final int toReduce = change;
					super.malleableShrink(toReduce);
				} else if (order.equals("expand")) {
					final List<String> hostnames = new ArrayList<>();
					final int toIncrease = change;
					for (int i = 0; i < toIncrease; i++) {
						hostnames.add(str[i + 2]);
					}
					super.malleableGrow(toIncrease, hostnames);
				} else {
					System.err.println(
							"Received unexpected order " + str[0] + ", expected <shrink> or <expand>. Ignoring ...");
				}
			} catch (final IOException e) {
				System.err.println("IO error while reading content from the socket");
				e.printStackTrace();
			}
		}
	}

	/**
	 * For class {@link SocketMalleableCommunicator}, starting means forking a
	 * thread to listen to incoming requests from the scheduler.
	 */
	@Override
	public void start() throws Exception {
		if (verbose) {
			System.err.println("SocketMalleableCommunicator start() method called, opening socket to listen to "
					+ schedulerIP + ":" + schedulerPort);
		}
		// We now fork a thread to listen to the socket and call the relevant method
		// when
		// an order from the scheduler is received.
		listenerThread = new Thread(this::listenToSocket);
		listenerThread.start();
	}

	/**
	 * For class {@link SocketMalleableCommunicator}, stopping consists in
	 * interrupting the thread listening for requests from the scheduler and
	 * cleaning up the objects used for communications.
	 */
	@Override
	public void stop() {
		if (verbose) {
			System.err.println(SocketMalleableCommunicator.class.getName()
					+ " stop() method called, closing socket and cleaning up.");
		}

		// Interrupt and release the thread listening on the socket
		listening = false;

		// If the start method was not called, listenerThread may be null
		if (listenerThread != null) {
			listenerThread.interrupt();
		}
	}

}
