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
package apgas.impl.elastic;

import apgas.Configuration;
import apgas.impl.GlobalRuntimeImpl;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of {@link ElasticCommunicator} based on a Socket.
 *
 * @author Patrick Finnerty
 */
public class SocketElasticCommunicator extends ElasticCommunicator {

  /**
   * Setting to set the IP from which connections may be accepted to receive malleable/evolving
   * orders
   */
  public static final String SCHEDULER_IP = "elastic_scheduler_ip";

  /**
   * Setting to set the port on which connections may be accepted to receive malleable/evolving
   * orders
   */
  public static final String SCHEDULER_Port = "elastic_scheduler_port";

  /** Value of property {@link #SCHEDULER_IP} for this execution */
  private final String schedulerIP;

  private final String serverSocketIP;

  /** Value of property {@link #SCHEDULER_Port} for this execution */
  private final int schedulerPort;

  private final int serverSocketPort;

  /** Socket used to receive orders from the scheduler */
  private final ServerSocket server;

  /**
   * Flag used to set verbose mode. Is initialized based on the value set for property {@link
   * Configuration#CONFIG_APGAS_VERBOSE_LAUNCHER}
   */
  private final boolean verbose;

  /** Thread in charge of (blockingly) waiting for incoming connections */
  private Thread listenerThread = null;

  /**
   * Boolean flag used when shutting down the {@link #listenerThread} upon the program terminating
   */
  private volatile boolean listening = true;

  /**
   * Socket of malleable/evolving connections
   *
   * <p>This socket is kept as a member for cases whereas malleable/evolving "shrink" order is
   * received and the hosts that were released need to be sent to the scheduler.
   */
  private Socket socket = null;

  /**
   * Constructor
   *
   * <p>The SocketElasticCommunicator users the properties {@link #SCHEDULER_IP} and {@link
   * #SCHEDULER_Port} to initialize its socket. This constructor is called using reflection as part
   * of a malleable or evolving APGAS execution.
   *
   * @throws Exception if thrown during the socket setup.
   */
  public SocketElasticCommunicator() throws Exception {
    // Obtain the IP/Port of the scheduler to establish a connection
    final Properties props = System.getProperties();
    if (!props.containsKey(SCHEDULER_IP) || !props.containsKey(SCHEDULER_Port)) {
      throw new Exception(
          "Cannot create the SocketElasticCommunicator, either the IP or the port of the scheduler was not set");
    }
    schedulerIP = System.getProperty(SCHEDULER_IP);
    // local:
    if (schedulerIP.equals("127.0.0.1")) {
      serverSocketIP = "127.0.0.1";
      // cluster:
    } else {
      serverSocketIP = GlobalRuntimeImpl.ip;
      // Alternative:
      // serverSocketIP = Inet4Address.getLocalHost().getHostAddress();
    }

    serverSocketPort = Integer.parseInt(System.getProperty(SCHEDULER_Port));
    schedulerPort = Integer.parseInt(System.getProperty(SCHEDULER_Port));

    server = new ServerSocket();
    server.bind(new InetSocketAddress(serverSocketIP, serverSocketPort));

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
   * Procedure which blocks on the socket to receive orders from the scheduler. When a message is
   * received, the thread running this procedure will call the appropriate superclass methods to
   * trigger the change in the program runtime.
   */
  private void listenToSocket() {
    while (listening) {
      try {
        socket = server.accept();
        final BufferedReader reader =
            new BufferedReader(new InputStreamReader(socket.getInputStream()));
        final String line = reader.readLine();
        if (verbose) {
          System.out.println("SocketElasticCommunicator received: " + line);
        }

        if (!listening) break;
        synchronized (lock) {
          // Interpret received order and call the appropriate procedure
          final String[] str = line.split(" ");
          final String order = str[0];
          final int change = Integer.parseInt(str[1]);
          if (order.equals("shrink")) {
            if (Configuration.CONFIG_APGAS_ELASTIC.get().equals("evolving")) {
              // Currently, should never happen
            } else {
              long before = System.nanoTime();
              super.malleableShrink(change);
              long shrinkTime = System.nanoTime() - before;
              sendToScheduler("shrinkTimeAPGAS;" + change + ";" + shrinkTime);
            }
          } else if (order.equals("grow")) {
            final List<String> hostnames = new ArrayList<>();
            hostnames.addAll(Arrays.asList(str).subList(2, change + 2));
            long before = System.nanoTime();
            super.elasticGrow(change, hostnames);
            long growTime = System.nanoTime() - before;
            sendToScheduler("growTimeAPGAS;" + change + ";" + growTime);
            GlobalRuntimeImpl.getRuntime().EVOLVING.openGrowRequest.set(false);
          } else {
            System.err.println(
                "Received unexpected order "
                    + str[0]
                    + ", expected <shrink> or <grow>. Ignoring ...");
          }
          // We only allow one elastic change per second,
          // which makes the runtime system much more stable
          TimeUnit.SECONDS.sleep(1);
        }
      } catch (SocketException | NullPointerException ignored) {
        System.err.println("Socket was Closed");
      } catch (final Exception e) {
        System.err.println("IO error while reading content from the socket");
        e.printStackTrace();
      }
    }
  }

  /**
   * For class {@link SocketElasticCommunicator}, starting means forking a thread to listen to
   * incoming requests from the scheduler.
   */
  @Override
  public void start() throws Exception {
    if (verbose) {
      System.err.println(
          "SocketElasticCommunicator start() method called, opening socket to listen to "
              + serverSocketIP
              + ":"
              + serverSocketPort);
    }
    // We now fork a thread to listen to the socket and call the relevant method
    // when an order from the scheduler is received.
    listenerThread = new Thread(this::listenToSocket);
    listenerThread.start();

    sendToScheduler("APGAS-Ready");
  }

  /**
   * sends some text to the scheduler program and appends ":serverSocketIP" (important as our
   * scheduler parses the string this way)
   *
   * @param message sent to the scheduler program
   */
  public void sendToScheduler(final String message) {
    if (System.getProperty(SCHEDULER_IP).equals("127.0.0.1")) {
      System.err.println("[sendToScheduler] scheduler ip is 127.0.0.1 ; return");
      return;
    }

    if (message.contains(":")) {
      System.err.println("[sendToScheduler] `:` is not allowed ; return");
      return;
    }

    try {
      Socket socketScheduler = new Socket(schedulerIP, schedulerPort);
      PrintWriter schedulerOut = new PrintWriter(socketScheduler.getOutputStream(), true);
      String text = message + ":" + serverSocketIP;
      schedulerOut.println(text);
      if (verbose) {
        System.err.println(text);
      }
      socketScheduler.close();
    } catch (Exception e) {
      System.err.println(
          "Exception can not connect to scheduler socket, schedulerIP="
              + schedulerIP
              + ", schedulerPort="
              + schedulerPort);
      e.printStackTrace();
    }
  }

  /**
   * For class {@link SocketElasticCommunicator}, stopping consists in interrupting the thread
   * listening for requests from the scheduler and cleaning up the objects used for communications.
   */
  @Override
  public void stop() {
    if (verbose) {
      System.err.println(
          SocketElasticCommunicator.class.getName()
              + " stop() method called, closing socket and cleaning up.");
    }

    // Interrupt and release the thread listening on the socket
    listening = false;

    // If the start method was not called, listenerThread may be null
    if (listenerThread != null) {
      try {
        socket.close();
        listenerThread.join();
      } catch (InterruptedException e) {
        System.err.println("Cant join Thread listenToSocket");
        e.printStackTrace();
      } catch (IOException e) {
        System.err.println("Cant close Socket");
        e.printStackTrace();
      }
    }
  }

  @Override
  public void interrupt() {
    if (verbose) {
      System.err.println(
          SocketElasticCommunicator.class.getName()
              + " stop() method called, closing socket and cleaning up.");
    }

    // Interrupt and release the thread listening on the socket
    listening = false;
    if (listenerThread != null) {
      listenerThread.interrupt();
    }
  }
}
