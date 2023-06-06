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

package apgas.util;

import static apgas.Constructs.async;
import static apgas.Constructs.asyncAt;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;

import java.io.Serializable;
import java.util.Collection;

import apgas.DeadPlaceException;
import apgas.Place;
import apgas.SerializableCallable;

/**
 * The {@link GlobalRef} class implements APGAS global references using
 * GlobalIDs.
 *
 * <p>
 * A global reference can be serialized among places. In each place it resolves
 * to zero or one object local to the place.
 *
 * @param <T> the type of the reference
 */
public class GlobalRef<T> implements Serializable {

	private static final long serialVersionUID = 4462229293688114477L;

	private static final Object UNDEFINED = new Object();

	/** The {@link GlobalID} instance for this {@link GlobalRef} instance. */
	private final GlobalID id;

	/** The collection of places used to construct the {@link GlobalRef} if any. */
	private final transient Collection<? extends Place> places;

	/**
	 * Constructs a {@link GlobalRef} over a collection of places.
	 *
	 * <p>
	 * This global reference is valid at any place in the given collection. The
	 * target of the reference in each place is obtained by evaluating {@code f} in
	 * each place at construction time.
	 *
	 * <p>
	 * This construction is a distributed operation with an implicit finish. The
	 * invocation will only return after all tasks transitively spawned by {@code f}
	 * have completed.
	 *
	 * @param places      a collection of places with no repetition
	 * @param initializer the function to evaluate to initialize the objects
	 */
	public GlobalRef(Collection<? extends Place> places, SerializableCallable<T> initializer) {
		id = new GlobalID();
		this.places = places;
		finish(() -> {
			for (final Place p : places) {
				try {
					asyncAt(p, () -> {
						id.putHere(initializer.call());
					});
				} catch (final DeadPlaceException e) {
					async(() -> {
						throw e;
					});
				}
			}
		});
	}

	/**
	 * Constructs a {@link GlobalRef} to the given object.
	 *
	 * @param t the target of the global reference
	 */
	public GlobalRef(T t) {
		if (t instanceof PlaceLocalObject) {
			id = ((PlaceLocalObject) t).id;
		} else {
			id = new GlobalID();
			id.putHere(t);
		}
		places = null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object that) {
		return that instanceof GlobalRef && id == ((GlobalRef) that).id;
	}

	/**
	 * Frees a {@link GlobalRef}.
	 *
	 * <p>
	 * Must be called from the place where this {@link GlobalRef} was instantiated.
	 *
	 * <p>
	 * Freeing a global reference removes the mapping from it's ID to local objects
	 * in each place where is was initially defined.
	 *
	 * <p>
	 * Failing to invoke this method on a {@link GlobalRef} instance will prevent
	 * the collection of the target objects of this global reference even after the
	 * global reference itself has been collected.
	 *
	 * @throws BadPlaceException if not invoked from the home place of the global
	 *                           reference
	 */
	public void free() {
		if (!id.home.equals(here())) {
			throw new BadPlaceException();
		}
		if (places == null) {
			id.removeHere();
		} else {
			id.remove(places);
		}
	}

	/**
	 * Dereferences this {@link GlobalRef} instance at the current place.
	 *
	 * @return the target object of the reference
	 * @throws BadPlaceException if the reference is not valid here
	 */
	@SuppressWarnings("unchecked")
	public T get() {
		final Object t = id.getOrDefaultHere(UNDEFINED);
		if (t == UNDEFINED) {
			throw new BadPlaceException();
		}
		return (T) t;
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	/**
	 * Returns the home {@link Place} of this {@link GlobalRef} instance.
	 *
	 * @return a place
	 */
	public Place home() {
		return id.home;
	}

	/**
	 * Sets the target object for this {@link GlobalRef} instance at the current
	 * place.
	 *
	 * @param t the target of the global reference
	 */
	public void set(T t) {
		id.putHere(t);
	}

	@Override
	public String toString() {
		return "ref(" + id.gid() + ")";
	}

	/**
	 * Removes the target object for this {@link GlobalRef} instance at the current
	 * place.
	 */
	public void unset() {
		id.removeHere();
	}
}
