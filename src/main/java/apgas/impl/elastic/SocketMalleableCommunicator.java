package apgas.impl.elastic;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Properties;

public class SocketMalleableCommunicator extends MalleableCommunicator {

    public static final String SCHEDULER_IP = "malleable_scheduler_ip";
    public static final String SCHEDULER_Port = "malleable_scheduler_port";
    
    private PrintWriter writer = null;
    private PrintWriter reader = null;
    private ServerSocket server = null;
    private Socket socket = null;
    
    private String schedulerIP;
    private int schedulerPort;
    
    public SocketMalleableCommunicator() {
        // Obtain the IP/Port of the scheduler to establish a connection
        Properties props = System.getProperties();
        if (!props.containsKey(SCHEDULER_IP) || !props.containsKey(SCHEDULER_Port)) {
            throw new RuntimeException("Cannot create the SocketMalleableCommunicator, either the IP or the port of the scheduler was not set");
        }
        schedulerIP = System.getProperty(SCHEDULER_IP);
        schedulerPort = Integer.parseInt(System.getProperty(SCHEDULER_Port));
        
//        server = new ServerSocket();
//        server.bind(new InetSocketAddress(schedulerIP, schedulerPort));
    }

    /**
     * For class {@link SocketMalleableCommunicator}, starting means forking a thread to listen to incoming requests from the scheduler. 
     */
    @Override
    public void start() {
        // TODO Auto-generated method stub
    	System.err.println("SocketMalleableCommunicator start() method called, opening socket to listen to " + schedulerIP + ":" + schedulerPort);
    }

    /**
     * For class {@link SocketMalleableCommunicator}, stopping consists in interrupting the thread listening for requests from the scheduler and cleaning up the objects used for communications.
     */
    @Override
    public void stop() {
        // TODO Auto-generated method stub
    	System.err.println("SockerMalleableCommunicator stop() method called, closing socket and cleaning up.");
    }

    @Override
    protected void hostReleased(List<String> hosts) {
        try {
            this.writer = new PrintWriter(socket.getOutputStream(), true);
            for (String hostName : hosts) {
              writer.println(hostName);
            }
          } catch (Exception e) {
            // e.printStackTrace();
          }
    }

}
