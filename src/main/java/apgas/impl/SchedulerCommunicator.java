package apgas.impl;

import apgas.util.SchedulerMessages;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class SchedulerCommunicator {
  // スケジューラとやり取りするクラス
  // スケジューラからの命令フォーマットの例："expand 3 node1 node1 node2"
  public SchedulerMessages message;
  public int numMallPlaces = 0;
  private ServerSocket server = null;
  private Socket socket = null;
  private BufferedReader reader = null;
  private PrintWriter writer = null;
  public String hostname = "localhost";
  public Integer port = 8080;

  public SchedulerCommunicator(String hostname, Integer port) {
    this.hostname = hostname;
    this.port = port;
    try {
      this.server = new ServerSocket();
      this.server.bind(new InetSocketAddress(hostname, port));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public SchedulerMessages receiveSchedulerMessage() {
    try {
      message = new SchedulerMessages();
      this.socket = server.accept();
      this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      String line = reader.readLine();
      String str[] = line.split(" ");
      System.out.println("=======================================");
      System.out.println("        message received : " + str[0]);
      System.out.println("=======================================");
      message.setBehavior(str[0]);
      message.setNumMallPlaces(Integer.parseInt(str[1]));
      ArrayList<String> hostNames = new ArrayList<String>();
      for (int i = 2; i < str.length; i++) {
        hostNames.add(str[i]);
      }
      message.setHostNames(hostNames);
    } catch (Exception e) {
      e.printStackTrace();
    } /*
       * finally {
       * try {
       * if (this.reader != null) this.reader.close();
       * if (this.socket != null) this.socket.close();
       * if (this.server != null) this.server.close();
       * } catch (Exception e) {
       * e.printStackTrace();
       * }
       * }
       */
    return message;
  }

  public void sendRemovedHosts(ArrayList<String> hosts) {
    try {
      this.writer = new PrintWriter(socket.getOutputStream(), true);
      for (String hostName : hosts) {
        writer.println(hostName);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public boolean closSocket() throws IOException {
    if (this.writer != null) this.writer.close();
    if (this.reader != null) this.reader.close();
    if (this.server != null) this.server.close();
    if (this.socket != null) this.socket.close();
    return true;
  }
}
