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
package apgas.impl.elastic;

import static apgas.Constructs.asyncAt;
import static apgas.Constructs.defineMalleableHandle;
import static apgas.Constructs.finish;
import static apgas.Constructs.places;

import java.util.ArrayList;
import java.util.List;

import apgas.Place;

/**
 * Simplistic program illustrating how to implement a malleable program
 *
 * @author Patrick Finnerty
 *
 */
public class DummyApplication {

	static class DummyHandler implements MalleableHandler {

		private static final long serialVersionUID = 4003743679074722952L;

		@Override
		public void postGrow(int nbPlaces, List<? extends Place> continuedPlaces, List<? extends Place> newPlaces) {
			System.out.println("Handler received notification that the grow operation is now complete");
			System.out.print("Continued places: [");
			for (final Place p : continuedPlaces) {
				System.out.print(p + " ");
			}
			System.out.println("]");

			System.out.print("New places: [");
			for (final Place p : newPlaces) {
				System.out.print(p + " ");
			}
			System.out.println("]");
		}

		@Override
		public void postShrink(int nbPlaces, List<? extends Place> removedPlaces) {
			System.out.println("Handler received notification that the shrink operation is now complete");
			System.out.print("Removed places: [");
			for (final Place p : removedPlaces) {
				System.out.print(p + " ");
			}
			System.out.println("]");
		}

		@Override
		public void preGrow(int nbPlaces) {
			System.out.println("Handler received notification that places will increase by " + nbPlaces);
		}

		@Override
		public List<Place> preShrink(int nbPlaces) {
			System.out.println("Handler received request to reduce places by " + nbPlaces);
			final List<? extends Place> nowPlaces = places();
			final List<Place> toRelease = new ArrayList<>(nbPlaces);
			for (final Place p : nowPlaces) {
				if (p.id == 0) {
					continue;
				}
				toRelease.add(p);
				System.out.println("Handler releases " + p);
				if (toRelease.size() == nbPlaces) {
					break;
				}
			}
			return toRelease;
		}

	}

	/**
	 * Dummy main which performs no computation and just waits till the specified
	 * time (in seconds) elapses
	 *
	 * @param args time to elapse in seconds
	 */
	public static void main(String[] args) {
		final int toElapse = Integer.parseInt(args[0]);

		finish(() -> {
			for (final Place p : places()) {
				asyncAt(p, () -> {
					System.out.println(p + " is running.");
				});
			}
		});

		// Enable malleability by defining the handler
		defineMalleableHandle(new DummyHandler());

		// Start dummy computation for the indicated time
		final long startWait = System.nanoTime();
		while (System.nanoTime() - startWait < 1e9 * toElapse) {
			try {
				Thread.sleep(100);// Wait in 100ms increments
			} catch (final InterruptedException e) {
				// Do nothing in case of exception
			}
		}

		System.out.println(toElapse + "s have elapsed, quiting");
	}
}
