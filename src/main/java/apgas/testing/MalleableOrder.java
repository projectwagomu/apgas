package apgas.testing;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import apgas.impl.elastic.SocketMalleableCommunicator;

/**
 * Simple main used to send malleable shrink/grow orders to a running
 * malleable APGAS program using the {@link SocketMalleableCommunicator} as the
 * communicator
 *
 * @author Kanzaki
 *
 */
public class MalleableOrder {

	private static final String DEFAULT_IP = "127.0.0.1";
	private static final String DEFAULT_PORT = "8081";

	private static final String JOB_IP = "job.ip";
	private static final String JOB_PORT = "job.port";

	/**
	 * This main takes different arguments depending on the type of order to
	 * sent to the running malleable program.<br>
	 * <h4>shrink case</h4> The arguments expected are:
	 * <ul>
	 * <li>shrink
	 * <li>number of host to release
	 * </ul>
	 * <p>
	 * This program then expects to receive the released hosts from the running
	 * program, one host per line. These hostnames are printed on the standard
	 * output of this program.
	 * <h4>grow case</h4> The arguments expected are:
	 * <ul>
	 * <li>grow
	 * <li>number of hosts to grow by
	 * <li>the hosts on which to spawn a new process
	 * </ul>
	 *
	 * @param args program arguments
	 */
	public static void main(String[] args) {
		// Parse the configuration
		final String jobIp = System.getProperty(JOB_IP, DEFAULT_IP);
		final int jobPort = Integer.parseInt(System.getProperty(JOB_PORT, DEFAULT_PORT));

		Socket socket = null;
		PrintWriter writer = null;

		try {
			socket = new Socket(jobIp, jobPort);
			writer = new PrintWriter(socket.getOutputStream(), true);
			// Send the order as it was received by this program
			String str = new String();
			for (final String arg : args) {
				str += (arg + " ");
			}
			writer.println(str);

			// If shrink order, then read the hosts that were freed
			if (args[0].equals("shrink")) {
				BufferedReader reader;
				reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				for (int i = 0; i < Integer.parseInt(args[1]); i++) {
					final String line = reader.readLine();
					System.out.println("[MalleableOrder] The hosts freed were: " + line);
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (socket != null) {
					socket.close();
				}
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}
}
