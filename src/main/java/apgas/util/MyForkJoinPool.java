package apgas.util;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class MyForkJoinPool extends ForkJoinPool {

	public MyForkJoinPool(int parallelism, int maxParallelism, ForkJoinWorkerThreadFactory factory,
			Thread.UncaughtExceptionHandler handler, boolean asyncMode) {
		super(parallelism, factory, handler, asyncMode, parallelism, maxParallelism, 1, null, 60_000L,
				TimeUnit.MILLISECONDS);
	}
}
