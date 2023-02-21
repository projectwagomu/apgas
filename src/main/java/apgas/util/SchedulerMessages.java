package apgas.util;

import java.util.List;

public class SchedulerMessages {
  private String behavior; // expand, shrink
  private List<String> hostNames; // placeを動作させるホスト
  private Integer numAddHosts; // 追加するホストの数
  private Integer numMallPlaces; // 追加するor減少するplaceの数

  public SchedulerMessages() {}

  public SchedulerMessages(String behavior, Integer numMallPlaces) {
    this.behavior = behavior;
    this.numMallPlaces = numMallPlaces;
  }

  public SchedulerMessages(
      String behavior, List<String> hostNames, Integer numMallPlaces, Integer numAddHosts) {
    this.behavior = behavior;
    this.numMallPlaces = numMallPlaces;
    this.numAddHosts = numAddHosts;
    this.hostNames = hostNames;
  }

  public int getBehaviorAsNum() {
    switch (this.behavior) {
      case "expand":
        return 0;
      case "shrink":
        return 1;
      default:
        return 2;
    }
  }

  public String getBehavior() {
    return behavior;
  }

  public List<String> getHostNames() {
    return hostNames;
  }

  public Integer getNumMallPlaces() {
    return numMallPlaces;
  }

  public Integer getNumAddHosts() {
    return numAddHosts;
  }

  public void setBehavior(String behavior) {
    this.behavior = behavior;
  }

  public void setHostNames(List<String> hostNames) {
    this.hostNames = hostNames;
  }

  public void setNumMallPlaces(Integer numMallPlaces) {
    this.numMallPlaces = numMallPlaces;
  }

  public void setNumAddHosts(Integer numAddHosts) {
    this.numAddHosts = numAddHosts;
  }
}
