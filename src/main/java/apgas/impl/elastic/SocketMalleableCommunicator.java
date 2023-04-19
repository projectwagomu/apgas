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

public class SocketMalleableCommunicator extends MalleableCommunicator {

	public static final String SCHEDULER_IP = "malleable_scheduler_ip";
	public static final String SCHEDULER_Port = "malleable_scheduler_port";

	private PrintWriter writer = null;
	private BufferedReader reader = null;
	/** Socket used to receive orders from the scheduler */
	private ServerSocket server = null;
	private Socket socket = null;

	private String schedulerIP;
	private int schedulerPort;

	private Thread listenerThread = null;

	private volatile boolean listening = true;

	public SocketMalleableCommunicator() throws Exception {
		// Obtain the IP/Port of the scheduler to establish a connection
		Properties props = System.getProperties();
		if (!props.containsKey(SCHEDULER_IP) || !props.containsKey(SCHEDULER_Port)) {
			throw new Exception("Cannot create the SocketMalleableCommunicator, either the IP or the port of the scheduler was not set");
		}
		schedulerIP = System.getProperty(SCHEDULER_IP);
		schedulerPort = Integer.parseInt(System.getProperty(SCHEDULER_Port));

		server = new ServerSocket();
		server.bind(new InetSocketAddress(schedulerIP, schedulerPort));
	}

	/**
	 * Procedure which blocks on the socket to receive orders from the scheduler.
	 * When a message is received, the thread running this procedure 
	 */
	private void listenToSocket() {
		while (listening) {
			try {
				socket = server.accept();
				reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String line = reader.readLine();
				System.out.println("SocketMalleableCommunicator received: " + line);

				// Interpret received order and call the appropriate procedure
				String str[] = line.split(" ");
				String order = str[0];
				int change = Integer.parseInt(str[1]);
				if (order.equals("shrink")) {
					int toReduce = change;
					super.malleableShrink(toReduce);
				} else if (order.equals("expand")){
					List<String> hostnames = new ArrayList<>();
					int toIncrease = change;
					for (int i=0; i<toIncrease; i++) {
						hostnames.add(str[i+2]);
					}
					super.malleableGrow(toIncrease, hostnames);
				} else {
					System.err.println("Received unexpected order " + str[0]+", expected <shrink> or <expand>. Ignoring ...");
				}
			} catch (IOException e) {
				System.err.println("IO error while reading content from the socket");
				e.printStackTrace();
			} 
		}
	}

	/**
	 * For class {@link SocketMalleableCommunicator}, starting means forking a thread to listen to incoming requests from the scheduler. 
	 */
	@Override
	public void start() throws Exception {
		System.err.println("SocketMalleableCommunicator start() method called, opening socket to listen to " + schedulerIP + ":" + schedulerPort);
		// We now fork a thread to listen to the socket and call the relevant method when 
		// an order from the scheduler is received.
		listenerThread = new Thread(()->listenToSocket());
		listenerThread.start();
	}

	/**
	 * For class {@link SocketMalleableCommunicator}, stopping consists in interrupting the thread listening for requests from the scheduler and cleaning up the objects used for communications.
	 */
	@Override
	public void stop() {
		System.err.println("SockerMalleableCommunicator stop() method called, closing socket and cleaning up.");

		// Interrupt and release the thread listening on the socket
		listening = false;
		
		// If the start method was not called, listenerThread may be null
		if (listenerThread != null) {
			listenerThread.interrupt();
		}
	}

	@Override
	protected void hostReleased(List<String> hosts) {
		for (String s : hosts) {
			System.err.println("Released host: " + s);
		}
		try {
			this.writer = new PrintWriter(socket.getOutputStream(), true);
			for (String hostName : hosts) {
				writer.println(hostName);
			}
		} catch (Exception e) {
			System.err.println("Encountered issue while indicating the released hosts to the scheduler");
			e.printStackTrace();
		}
	}

}
