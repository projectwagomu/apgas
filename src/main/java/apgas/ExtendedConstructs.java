package apgas;

import apgas.impl.elastic.MalleableHandler;

public class ExtendedConstructs {

	/**
	 * Method used to define the handler which will be responsible for
	 * interacting with the running program so that it correctly handles the
	 * transitions between place count changes.
	 * The program becomes malleable as a result of calling this method. This is
	 * because a socket is opened as a result of this call, effectively making
	 * it possible to receive shrink of grow orders from the job scheduler. 
	 * @param handler the handler now in charge of handling malleable shrink and
	 * grow orders from the job scheduler
	 */
	public static void defineMalleableHandle(MalleableHandler handler) {
		GlobalRuntime.getRuntimeImpl().setMalleableHandler(handler);
	}
}
