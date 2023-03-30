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
    
    
    SocketMalleableCommunicator() {
        // Obtain the IP/Port of the scheduler to establish a connection
        Properties props = System.getProperties();
        if (!props.containsKey(SCHEDULER_IP) || !props.containsKey(SCHEDULER_Port)) {
            throw new RuntimeException("Cannot create the SocketMalleableCommunicator, either the IP or the port of the scheduler was not set");
        }
        String schedulerIP = System.getProperty(SCHEDULER_IP);
        int schedulerPort = Integer.parseInt(System.getProperty(SCHEDULER_Port));
        
//        server = new ServerSocket();
//        server.bind(new InetSocketAddress(schedulerIP, schedulerPort));
    }
    
    @Override
    public void start() {
        // TODO Auto-generated method stub

    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub

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
