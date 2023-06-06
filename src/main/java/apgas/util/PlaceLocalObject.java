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

import static apgas.Constructs.finish;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collection;

import apgas.Constructs;
import apgas.DeadPlaceException;
import apgas.Place;
import apgas.SerializableCallable;

/**
 * A {@link PlaceLocalObject} instance maintains an implicit map from places to
 * objects.
 *
 * <p>
 * Serializing a place local object across places does not replicate the object
 * as usual but instead transfer the {@link GlobalID} of the place local object
 * instance. This id is resolved at the destination place to the object local to
 * the place.
 */
public class PlaceLocalObject implements Serializable {

	private static final class ObjectReference implements Serializable {

		private static final long serialVersionUID = -2416972795695833335L;

		private final GlobalID id;

		private ObjectReference(GlobalID id) {
			this.id = id;
		}

		private Object readResolve() throws ObjectStreamException {
			return id.getHere();
		}
	}

	/**
	 * Returns the {@link GlobalID} of the given {@link PlaceLocalObject} instance.
	 *
	 * @param object a place local object
	 * @return the global ID of this object
	 */
	public static GlobalID getId(PlaceLocalObject object) {
		return object.id;
	}

	/**
	 * Constructs a {@link PlaceLocalObject} instance.
	 *
	 * @param <T>         the type of the constructed place local object
	 * @param places      a collection of places with no repetition
	 * @param initializer the function to evaluate to initialize the objects
	 * @return the place local object instance
	 */
	@SuppressWarnings("unchecked")
	public static <T extends PlaceLocalObject> T make(Collection<? extends Place> places,
			SerializableCallable<T> initializer) {
		final GlobalID idLocal = new GlobalID();
		try {
			finish(() -> {
				for (final Place p : places) {
					Constructs.asyncAt(p, () -> {
						final T t = initializer.call();
						t.id = idLocal;
						idLocal.putHere(t);
					});
				}
			});
		} catch (final DeadPlaceException e) {
			idLocal.remove(places);
			throw e;
		}
		return (T) idLocal.getHere();
	}

	/** The {@link GlobalID} of this {@link PlaceLocalObject} instance. */
	public GlobalID id; // package private

	/**
	 * Constructs a reference to this {@link PlaceLocalObject} instance.
	 *
	 * @return the object reference
	 * @throws ObjectStreamException N/A
	 */
	public Object writeReplace() throws ObjectStreamException {
		return new ObjectReference(id);
	}
}
