package apgas.util;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * Custom pool used to manage the APGAS activities
 */
public class MyForkJoinPool extends ForkJoinPool {
	/**
	 * Constructor
	 *
	 * @param parallelism    pool parallelism
	 * @param maxParallelism maximum number of threads
	 * @param factory        thread factory for this pool
	 * @param handler        handler in case the running thread does not terminate
	 *                       correctly
	 */
	public MyForkJoinPool(int parallelism, int maxParallelism, ForkJoinWorkerThreadFactory factory,
			Thread.UncaughtExceptionHandler handler) {
		super(parallelism, factory, handler, false, parallelism, maxParallelism, 1, null, 60_000L,
				TimeUnit.MILLISECONDS);
	}
}
