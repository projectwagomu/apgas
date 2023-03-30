package apgas.impl.elastic;

import static apgas.Constructs.*;

import java.util.ArrayList;
import java.util.List;

import apgas.Constructs;
import apgas.Place;
import apgas.impl.GlobalRuntimeImpl;

/**
 * Class in charge of implementing the communication protocol with the scheduler
 * on the APGAS runtime.
 * This class serves as the recipient for malleable requests coming from the
 * scheduler and the sender for evolving requests coming from running programs.
 * @author Patrick Finnerty
 */
public abstract class MalleableCommunicator {

	/**
	 * Method called when the running program moves into a state where it is
	 * capable of changing its number of running hosts and as a result may
	 * receive orders from the scheduler.
	 */
	public abstract void start();
	
	/**
	 * Method called when the running program is about to terminate. This method
	 * gives the opportunity to the communicator to shutdown cleanly.
	 */
	public abstract void stop();
	
	/**
	 * Method to call by the extending class when a shrink order is received
	 * from the scheduler.
	 * @param nbPlacesToFree number of places to release
	 */
	@SuppressWarnings("unchecked")
	final protected void malleableShrink(int nbPlacesToFree) {
		// Perform the user-defined pre-shrink tasks
		final List<Place> toRelease = GlobalRuntimeImpl.getRuntime().malleableHandler.preShrink(nbPlacesToFree);

		// Obtain the hostnames of the places to release and shutdown these places
		ArrayList<String> hosts = new ArrayList<>();
		for (Place p : toRelease) {
			String hostname = at(p,()->{
				return System.getenv("HOSTNAME");
			});
			hosts.add(hostname);
		}
		// Shrink
		// TODO
		
		// Inform the scheduler of the released hosts
		hostReleased(hosts);
		
		// Inform the running program of the end of the operation
		List<Place> places = (List<Place>) Constructs.places();
		GlobalRuntimeImpl.getRuntime().malleableHandler.postShrink(places.size(), places);
	}
	
	/**
	 * Method to call by the extending class when a grow order is received from
	 * the scheduler.
	 * @param nbPlacesToGrow number of places to grow by
	 * @param hosts hosts to use to spawn new places
	 */
	final protected void malleableGrow(int nbPlacesToGrow, List<String> hosts) {
		// Perform the user-defined pre-grow tasks
		GlobalRuntimeImpl.getRuntime().malleableHandler.preGrow(nbPlacesToGrow);
		
		// Grow
		
		// Inform the running program of the end of this grow operation
		// TODO
		//GlobalRuntimeImpl.getRuntime().malleableHandler.postGrow(nbPlaces, currentPlaces, newPlaces);
	}
	
	/**
	 * Informs the scheduler that the hosts given as argument were released
	 * @param placesReleased places released following a shrink order
	 */
	abstract protected void hostReleased(List<String> hosts);
}
