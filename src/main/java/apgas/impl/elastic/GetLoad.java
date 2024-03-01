package apgas.impl.elastic;

/**
 * Get load Interface for different load variants via getLoad classes - cpu and task load
 * implemented so far.
 */
public interface GetLoad {
  double getLoad() throws InterruptedException;
}
