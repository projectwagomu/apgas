package apgas.impl.elastic;

import java.io.Serializable;
import java.util.List;

import apgas.Constructs;
import apgas.ExtendedConstructs;
import apgas.Place;

/**
 * Interface which defines what to do when a malleable (shrink or grow) order
 * comes from the scheduler. The four method presented here are used to define
 * what needs to be done before and after the running program either increases
 * or decreases its number of running processes. As this depends on the program,
 * this interface was designed to be generic enough to allow for any
 * implementation.
 * <p>
 * Note that all the methods defined in this interface will be run from Place0.
 * If alterations to the running program need to be performed on other places,
 * the usual finish/asyncAt constructs provided by class {@link Constructs}.
 * <p>
 * Programmers willing to make their program malleable should define the handler
 * for their specific program using
 * {@link ExtendedConstructs#defineMalleableHandle(MalleableHandler)}
 * 
 * @author Patrick Finnerty
 *
 */
public interface MalleableHandler extends Serializable {

	/**
	 * Method called when a shrink order is received by the scheduler. In this
	 * method, all the preparations necessary to the program prior to the actual
	 * release of places should be performed before returning a list containing the
	 * list of places to release. The places returned in the list should be distinct
	 * and match the number of places indicated by the parameter given to the
	 * method.
	 * <p>
	 * Method {@link #postShrink(int, List)} will be called next.
	 * 
	 * @param nbPlaces number of places that have to be released
	 * @return the places that will be released
	 */
	public List<Place> preShrink(int nbPlaces);

	/**
	 * Method called prior to an increase in the number of processes in the running
	 * program. If any preparation is needed prior to the increase in the number of
	 * places is needed, they should be performed before this method returns.
	 * <p>
	 * Method {@link #postGrow(int, List, List)} will be called next.
	 * 
	 * @param nbPlaces number of places that will be added to the running program
	 */
	public void preGrow(int nbPlaces);

	/**
	 * Method called after the necessary number of places were removed from the
	 * running program. The distributed runtime has completed all its operations and
	 * the running program can now resume normal execution.
	 * 
	 * @param nbPlaces      number of places currently running
	 * @param removedPlaces list containing the places that were removed
	 */
	public void postShrink(int nbPlaces, List<? extends Place> removedPlaces);

	/**
	 * Method called after the necessary number of places were added to the running
	 * program. The distributed runtime has completed its adjustments and the
	 * running program can now resume normal execution.
	 * 
	 * @param nbPlaces        number of places currently running
	 * @param continuedPlaces list containing the places already present prior to
	 *                        the grow operation
	 * @param newPlaces       list containing the places that were added to the
	 *                        runtime
	 */
	public void postGrow(int nbPlaces, List<? extends Place> continuedPlaces, List<? extends Place> newPlaces);
}
