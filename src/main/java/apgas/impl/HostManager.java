package apgas.impl;

import apgas.Configuration;
import apgas.Place;
import apgas.util.ConsolePrinter;
import apgas.util.SchedulerMessages;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This Class maintains all Hosts of the Runtime to reuse the Hosts after removing some of the
 * Places. ATM we assume, that the initial given List of Hosts are reachable and usable until the
 * end of the whole execution.
 */
public class HostManager {

  /** Command to start new Places */
  final ArrayList<String> launchCommand = new ArrayList<>();
  /** The List of Hosts */
  private final List<Host> hosts;
  /** The local Host */
  private final Host localHost;

  private final boolean verboseLauncher = Configuration.APGAS_VERBOSE_LAUNCHER.get();
  /**
   * This atomic Integer is used to generate unique ID's for the Places. Start counting on 1,
   * because the Master node gets the value 0 as default
   */
  private final AtomicInteger placeIDGenerator;

  private final int strategy;

  final int numFirstPlaces = Configuration.APGAS_PLACES.get();

  String removedHostName = null;
  /**
   * Constructor
   *
   * @param hostnames The List of Hostnames to maintain
   */
  public HostManager(final List<String> hostnames, final String localhost) {
    // We assume that the first host in hostname is the localhost (master)
    this.hosts = new ArrayList<>();
    if (hostnames.size() > 0) {
      for (String hostname : hostnames) {
        addHost(hostname);
      }
      // for (Host h : this.hosts) h.maxPlacesPerHost = Integer.MAX_VALUE;

      this.hosts.get(0).attachPlace(new Place(0));
      this.localHost = this.hosts.get(0);
    } else if (localhost != null) {
      Host host =
          new Host(localhost, numFirstPlaces); // もともとはnumFirtsPlacesではなくInteger.MAX_VALUEになってた
      host.attachPlace(new Place(0));
      this.hosts.add(host);
      this.localHost = host;
    } else {
      this.localHost = null;
      System.err.println("[APGAS] HostManager: Error! No valid hostnames and no localhost!");
    }
    this.placeIDGenerator = new AtomicInteger(0);

    // this.strategy = Configuration.APGAS_HOSTMANAGER_STRATEGY.get();
    this.strategy = 0;
  }

  /** adds a host, if the Name exits, the value of maxPlacesPerHost is incremented */
  public void addHost(String hostname) {
    for (Host host : this.hosts) {
      if (host.hostName.equals(hostname)) {
        host.maxPlacesPerHost++;
        return;
      }
    }
    this.hosts.add(new Host(hostname, 1));
  }

  public void addHostFromMessage(SchedulerMessages message) {
    List<String> newHostNames = message.getHostNames();
    for (String newHostname : newHostNames) {
      addHost(newHostname);
    }
  }

  public List<String> getCopyOfLaunchCommand() {
    List<String> result = new ArrayList<>();
    for (String c : this.launchCommand) {
      result.add(c + "");
    }
    return result;
  }

  /**
   * gets all HostNames
   *
   * @return a List of all HostNames
   */
  public List<String> getHostNames() {
    List<String> result = new ArrayList<>();
    for (Host h : this.hosts) {
      result.add(h.hostName);
    }
    return result;
  }

  /**
   * retrieve the next possible Host to use for a new Place.
   *
   * @return next suitable Host to use for a new Place, could be null
   */
  public Host getNextHost() {
    Host nexHost = null;
    if (strategy == 1) { // Nodes are filled one after the other
      for (Host h : this.hosts) {
        if (h.attachedPlacesCount() < h.maxPlacesPerHost) {
          nexHost = h;
          break;
        }
      }
    } else if (strategy == 0) { // (stragety == 0) Places are added cyclical to the nodes (default)
      int difference = 0;
      for (Host h : this.hosts) {
        int diff = h.maxPlacesPerHost - h.attachedPlacesCount();
        if (diff > 0 && diff > difference) {
          difference = diff;
          nexHost = h;
        }
      }
    }
    if (verboseLauncher) {
      System.err.println("Hostmanager: getNextHost found host: " + nexHost.getHostName());
    }
    return nexHost;
  }

  // 最初，メッセージを受け取ったこのメソッドで任意のホストの追加と割り当てを
  // しようと思ったけど，呼び出し元の同期処理がよくわかんなかったからやめた.
  // 今はmaxPlacePerHostの値を増減させてやっているからこれは使ってない．
  public Host getNextHost(SchedulerMessages message) {
    Host nextHost = null;
    List<String> newHostNames = message.getHostNames();
    // addHost(hostName);
    for (String hostname : newHostNames) {
      addHost(hostname);
    }
    for (Host h : this.hosts) {
      int diff = h.maxPlacesPerHost - h.attachedPlacesCount();
      if (h.hostName.equals(message.getHostNames()) && diff > 0) {
        nextHost = h;
      }
    }
    if (nextHost == null) {
      int difference = 0;
      for (Host h : this.hosts) {
        int diff = h.maxPlacesPerHost - h.attachedPlacesCount();
        if (diff > 0 && diff > difference) {
          difference = diff;
          nextHost = h;
        }
      }
    }
    if (verboseLauncher) {
      System.err.println("Hostmanager: getNextHost found host: " + nextHost.getHostName());
    }
    return nextHost;
  }

  /**
   * detaches the given Place from the corresponding Host
   *
   * @param place The Place which should be detached from its Host.
   */
  public String detachFromHost(final Place place) {
    // findFirst should find always one or no entry, because a Place can only have one Host at a
    // Time
    Optional<Host> first =
        this.hosts.stream().filter(host -> host.attachedPlaces.contains(place)).findFirst();
    removedHostName = null;
    first.ifPresent(
        host -> {
          host.detachPlace(place);
          ConsolePrinter.getInstance().printlnWithoutAPGAS(place + " detached from " + host);
          removedHostName = host.getHostName();
        });
    return removedHostName;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("HostManager:\n");
    for (Host host : this.hosts) {
      stringBuilder.append(" " + host + "\n");
    }
    stringBuilder.append(" placeIDGenerator=" + this.placeIDGenerator.get());
    return stringBuilder.toString();
  }

  /**
   * generates the given amount of new PlaceID's
   *
   * @param count The amount of PlaceID's to generate
   * @return The List of generated PlaceID's
   */
  public List<Integer> generateNewPlaceIds(int count) {
    return Stream.generate(() -> placeIDGenerator.incrementAndGet())
        .limit(count)
        .collect(Collectors.toList());
  }

  public void decrementPlaceIds(List<Place> PlacesToBeRemove) {
    // for (int i = 0; i < PlacesToBeRemove.size(); i++) placeIDGenerator.decrementAndGet();
  }

  public void buildLaunchCommand(String masterIp, String classToStart) {
    this.launchCommand.add(Configuration.APGAS_JAVA.get());
    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    List<String> inputArguments = runtimeMXBean.getInputArguments();
    for (String a : inputArguments) {
      if (!a.contains("-Dapgas") && !a.contains("-Dglb")) {
        this.launchCommand.add(a);
      }
    }

    this.launchCommand.add("-Duser.dir=" + System.getProperty("user.dir"));
    this.launchCommand.add("-cp");
    this.launchCommand.add(System.getProperty("java.class.path"));
    for (final String property : System.getProperties().stringPropertyNames()) {
      if ((property.startsWith("apgas.") && !property.contains(".place.id"))
          || property.startsWith("glb")) {
        this.launchCommand.add("-D" + property + "=" + System.getProperty(property));
      }
    }
    this.launchCommand.add(
        "-D"
            + Configuration.APGAS_MASTER.getName()
            + "="
            + masterIp
            + ":"
            + Configuration.APGAS_PORT.get());
    this.launchCommand.add(classToStart);
  }

  /** This Class represents a Host with a Hostname and it's attached Places. */
  public static class Host {

    private final String hostName;
    private final List<Place> attachedPlaces;
    int maxPlacesPerHost;

    /**
     * Constructor
     *
     * @param hostName The Hostname of the Host
     */
    public Host(final String hostName, final int maxPlacesPerHost) {
      this.hostName = hostName;
      this.attachedPlaces = new ArrayList<>();
      this.maxPlacesPerHost = maxPlacesPerHost;
    }

    /**
     * attaches the given Place to this Host
     *
     * @param place The Place to attach to this Host
     */
    public void attachPlace(final Place place) {
      if (this.attachedPlacesCount() >= this.maxPlacesPerHost) {
        System.err.println(
            "[APGAS] HostManager: on Host "
                + this.hostName
                + " are now "
                + (this.attachedPlacesCount() + 1)
                + " attached, but maxPlacesPerHost="
                + this.maxPlacesPerHost);
      }
      this.attachedPlaces.add(place);
      ConsolePrinter.getInstance().printlnWithoutAPGAS(place + " attached from " + this);
    }

    /**
     * detaches the given Place from this Host
     *
     * @param place The Place to detach from this Host
     */
    public void detachPlace(final Place place) {
      this.attachedPlaces.remove(place);
      ConsolePrinter.getInstance().printlnWithoutAPGAS(place + "detached from " + this);
      this.maxPlacesPerHost--;
    }

    /**
     * get the number of attached Hosts
     *
     * @return the number of attached Places
     */
    public int attachedPlacesCount() {
      return this.attachedPlaces.size();
    }

    /**
     * get the Hostname of this Host
     *
     * @return the Hostname
     */
    public String getHostName() {
      return hostName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Host host = (Host) o;
      return hostName.equals(host.hostName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(hostName);
    }

    @Override
    public String toString() {
      return "hostName="
          + hostName
          + ", attachedPlaces="
          + attachedPlaces
          + ", maxPlacesPerHost="
          + this.maxPlacesPerHost;
    }
  }
}
