package apgas.impl.elastic;

import static apgas.Constructs.here;

import com.sun.management.OperatingSystemMXBean;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

/**
 * Class in charge of obtaining a cpu-based load on a place.
 *
 * @author Ashatar
 */
public class GetCpuLoad implements GetLoad, Serializable {
  /** Serial Version UID */
  private static final long serialVersionUID = -9066000640974753667L;

  /** Java MXBean for obtaining cpu usage of host */
  private static final OperatingSystemMXBean operatingSystemMXBean =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

  /** Max duration in seconds for a place to have a very low load. */
  private final long maxIdle = 10;

  /**
   * Samples taken for cpu load 20 samples to avoid spikes. Defaults to 20 samples.
   *
   * <p>In combination with samplesTimer this amounts to a load average of 1 second gathered by
   * getCpuLoad method.
   */
  private int samples;

  /**
   * Time between one sample and another for a cpu load. Defaults to 50 milliseconds.
   *
   * <p>In combination with samples, this amounts to a load average of 1 second gathered by
   * getCpuLoad method.
   */
  private int samplesTimer;

  /** Load value of this place. */
  private double load;

  /**
   * Elapsed time since this place has a very low load. Defaults to -1 if place has enough load.
   * Gets reset to -1 if a place only had a very low load for a short time but has a higher load
   * after a low.
   */
  private long lowLoadSince = -1;

  /** The time the place has been inactive. */
  private long inactive;

  /** Current time for comparison to noTasksSince. */
  private long now;

  /**
   * Method for calculating a cpu load on a place, will be called on each place by
   * cpuLoadEvaluation() method every few seconds defined by loadCheckInterval variable - default 1
   * second
   */
  @Override
  public double getLoad() throws InterruptedException {
    samples = 20;
    samplesTimer = 50;
    double hostCpu = 0;
    for (int i = 0; i < samples; i++) {
      hostCpu = hostCpu + (100 * operatingSystemMXBean.getSystemCpuLoad());
      TimeUnit.MILLISECONDS.sleep(samplesTimer);
    }
    hostCpu = hostCpu / samples;
    now = System.nanoTime();
    load = (double) Math.round(hostCpu * 100d) / 100;

    // Return 0 Load if a place hast load below 1% for 10 seconds
    if (here().id != 0 && load < 1) {
      if (lowLoadSince == -1) {
        lowLoadSince = System.nanoTime();
      }
      inactive = now - lowLoadSince;
      inactive /= 1e9;
      if (inactive > maxIdle) {
        return 0;
      } else {
        return load;
      }
    }
    lowLoadSince = -1;
    return load;
  }
}
