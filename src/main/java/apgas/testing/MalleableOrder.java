package apgas.testing;

import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

import apgas.impl.elastic.SocketMalleableCommunicator;

/**
 * Simple main used to send malleable shrink/expand orders to a running malleable APGAS program
 * using the {@link SocketMalleableCommunicator} as the communicator
 * @author Kanzaki
 *
 */
public class MalleableOrder {

	public static final String JOB_IP = "job.ip";
	public static final String JOB_PORT = "job.port";
	
	public static final String DEFAULT_IP = "localhost";
	public static final String DEFAULT_PORT = "8080";
	
	/**
	 * This main takes different arguments depending on the on the type of order to sent to the running malleable program.
	 * <p>
	 * <h1>shrink case</h1>
	 * The arguments expected are:
	 * <ul>
	 * <li>shrink
	 * <li>number of host to release
	 * </ul>
	 * <p>
	 * This program then expects to receive the released hosts from the running program, one host per line.
	 * These hostnames are printed on the standard output of this program.
	 * <p>
	 * <h1>grow case</h1>
	 * The arguments expected are:
	 * <ul>
	 * <li>grow
	 * <li>number of hosts to grow by
	 * <li>the hosts on which to spawn a new process
	 * </ul>
	 * <p>
	 * 
	 * 
	 * @param args program arguments
	 */
	public static void main(String[] args) {
		// Parse the configuration
		final String jobIp = System.getProperty(JOB_IP, DEFAULT_IP);
		final int jobPort = Integer.parseInt(System.getProperty(JOB_PORT, DEFAULT_PORT));
		
		try (Socket socket = new Socket(jobIp, jobPort);
				PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {
			String str = new String();
			for (int i = 0; i < args.length; i++) {
				str += (args[i] + " ");
			}
			writer.println(str);
			if (args[0].equals("shrink")) {
				BufferedReader reader;
				reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				for (int i = 0; i < Integer.parseInt(args[1]); i++) {
					String line = reader.readLine();
					System.out.println(line);
				}
			}
			return;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
