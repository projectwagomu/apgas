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

package apgas.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import apgas.DeadPlaceException;
import apgas.Place;
import apgas.util.GlobalID;

/**
 * The {@link ResilientFinish} class implements a finish construct resilient to
 * place failure.
 */
class ResilientFinish implements Serializable, Finish {

	/** A factory producing {@link ResilientFinish} instances. */
	static class Factory extends Finish.Factory {

		@Override
		ResilientFinish make(Finish parent) {
			return new ResilientFinish(parent);
		}
	}

	private static final long serialVersionUID = -8238404708052769991L;

	/** The unique id of this finish instance. */
	protected GlobalID id;

	/**
	 * Allocates but does not construct a resilient finish instance (for lazy
	 * initialization).
	 */
	protected ResilientFinish() {
	}

	/**
	 * Constructs a resilient finish instance.
	 *
	 * @param parent the parent finish instance
	 */
	private ResilientFinish(Finish parent) {
		init(parent);
	}

	@Override
	public void addSuppressed(Throwable exception) {
		final int here = GlobalRuntimeImpl.getRuntime().here;
		final SerializableThrowable t = new SerializableThrowable(exception);
		exception.printStackTrace(System.out);
		ResilientFinishState.update(id, state -> {
			if (state == null || state.getDeads() != null && state.getDeads().contains(here)) {
				// finish thinks this place is dead, exit
				throw new DeadPlaceError();
			}
			if (state.getExceptions() == null) {
				state.setExceptions(new ArrayList<>());
			}
			state.addException(t);
			return state;
		});
	}

	@Override
	public boolean block() {
		final String reg = ResilientFinishState.addListener(this);
		synchronized (this) {
			while (!isDone()) {
				try {
					wait(1000);
				} catch (final InterruptedException e) {
				}
			}
		}
		ResilientFinishState.removeListener(reg);
		return true;
	}

	@Override
	public List<Throwable> exceptions() {
		final int here = GlobalRuntimeImpl.getRuntime().here;
		final List<SerializableThrowable> exceptions = ResilientFinishState.execute(id, entry -> {
			final ResilientFinishState state = entry.getValue();
			if (state == null || state.getDeads() != null && state.getDeads().contains(here)) {
				// parent finish thinks this place is dead, exit
				throw new DeadPlaceError();
			}
			entry.setValue(null);
			return state.getExceptions();
		});
		if (exceptions == null) {
			return null;
		}
		final List<Throwable> list = new ArrayList<>();
		for (final SerializableThrowable t : exceptions) {
			list.add(t.t);
		}
		return list;
	}

	/**
	 * Does the buld of the finish construction.
	 *
	 * @param parent the parent finish instance
	 */
	protected void init(Finish parent) {
		id = new GlobalID();
		final GlobalID pid = parent instanceof ResilientFinish ? ((ResilientFinish) parent).id : null;
		final int here = GlobalRuntimeImpl.getRuntime().here;
		ResilientFinishState.update(id, state -> new ResilientFinishState(pid, here));
		if (pid == null) {
			return;
		}
		ResilientFinishState.update(pid, state -> {
			if (state == null || state.getDeads() != null && state.getDeads().contains(here)) {
				// parent finish thinks this place is dead, exit
				throw new DeadPlaceError();
			}
			if (state.getDids() == null || !state.getDids().contains(id)) {
				if (state.getCids() == null) {
					state.setCids(new HashSet<>());
				}
				state.addCid(id);
			}
			return state;
		});
	}

	private boolean isDone() {
		final int here = GlobalRuntimeImpl.getRuntime().here;
		return ResilientFinishState.execute(id, false, // no need to apply on backup
				entry -> {
					final ResilientFinishState state = entry.getValue();
					if (state == null || state.getDeads() != null && state.getDeads().contains(here)) {
						// parent finish thinks this place is dead, exit
						throw new DeadPlaceError();
					}
					return state.getCounts().size() == 0 && (state.getCids() == null || state.getCids().isEmpty());
				});
	}

	@Override
	public boolean isReleasable() {
		return isDone();
	}

	@Override
	public void spawn(int p) {
		final int here = GlobalRuntimeImpl.getRuntime().here;
		ResilientFinishState.update(id, state -> {
			if (state == null || state.getDeads() != null && state.getDeads().contains(here)) {
				// finish thinks this place is dead, exit
				throw new DeadPlaceError();
			}
			if (state.getDeads() != null && state.getDeads().contains(p)) {
				// destination place has died, reject task
				System.out.println(here + " DeadPlaceException: destination place has died, reject task p=" + p);
				throw new DeadPlaceException(new Place(p));
			}
			state.incr(here, p);
			return state;
		});
	}

	@Override
	public void submit(int p) {
		final int here = GlobalRuntimeImpl.getRuntime().here;
		if (p == here) {
			// task originated here, no transit stage
			return;
		}
		ResilientFinishState.update(id, state -> {
			if (state == null || state.getDeads() != null && state.getDeads().contains(here)) {
				// finish thinks this place is dead, exit
				throw new DeadPlaceError();
			}
			if (state.getDeads() != null && state.getDeads().contains(p)) {
				// source place has died, refuse task but keep place alive
				System.out.println(
						here + " DeadPlaceException: source place has died, refuse task but keep place alive p=" + p);
				throw new DeadPlaceException(new Place(p));
			}
			state.decr(p, here);
			state.incr(here, here);
			return state;
		});
	}

	@Override
	public void tell() {
		final int here = GlobalRuntimeImpl.getRuntime().here;
		ResilientFinishState.submit(id, state -> {
			if (state == null || state.getDeads() != null && state.getDeads().contains(here)) {
				// finish thinks this place is dead, exit
				throw new DeadPlaceError();
			}
			state.decr(here, here);
			return state;
		});
	}

	@Override
	public void unspawn(int p) {
		final int here = GlobalRuntimeImpl.getRuntime().here;
		ResilientFinishState.submit(id, state -> {
			if (state == null || state.getDeads() != null && state.getDeads().contains(here)) {
				// finish thinks this place is dead, exit
				throw new DeadPlaceError();
			}
			if (state.getDeads() != null && state.getDeads().contains(p)) {
				// destination place has died, return
				return null;
			}
			state.decr(here, p);
			return state;
		});
	}
}
