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

import static apgas.Constructs.here;

import apgas.DeadPlaceException;
import apgas.Place;
import apgas.util.ExactlyOnceExecutor;
import apgas.util.GlobalID;
import apgas.util.IncrementalEntryValue;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.query.Predicate;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@link ResilientFinishState} class defines the entry associated with a finish object in the
 * resilient store.
 */
final class ResilientFinishState extends IncrementalEntryValue implements Serializable {

  /** ExactlyOnceExecutor for making sure EntryProcessors are only executed once per partition. */
  static final ExactlyOnceExecutor<GlobalID, ResilientFinishState> exactlyOnceExecutor =
      new ExactlyOnceExecutor<>();

  private static final long serialVersionUID = 756668504413905415L;

  /** The task counts. */
  private final Map<Long, Integer> counts = new HashMap<>();

  /** The ID of the parent resilient finish object if any. */
  private final GlobalID pid;

  /** The IDs of the live immediately nested finish objects. */
  private Set<GlobalID> cids;

  /** The set of places that have died during this finish execution. */
  private Set<Integer> deads;

  /** The IDs of the dead immediately nested finish objects. */
  private Set<GlobalID> dids;

  /** The exceptions reported to this finish so far. */
  private List<SerializableThrowable> exceptions;

  /** The largest place ID encountered so far. */
  private int max;

  /**
   * Constructs a resilient finish state.
   *
   * @param pid the ID of the parent resilient finish if any
   * @param p the place ID of the finish
   */
  ResilientFinishState(GlobalID pid, int p) {
    max = p;
    this.pid = pid;
    counts.put(index(p, p), 1);
  }

  /**
   * Registers a resilient store listener.
   *
   * <p>The finish instance is notified when its entry is updated or removed from the resilient
   * store.
   *
   * @param finish the finish instance to register
   * @return the unique id of the registration
   */
  static String addListener(ResilientFinish finish) {
    return GlobalRuntimeImpl.getRuntime()
        .resilientFinishMap
        .addEntryListener(new EntryUpdatedOrRemovedListener(finish), finish.id, false);
  }

  /**
   * Apply an entry processor to an entry.
   *
   * @param <T> the return type of the processor
   * @param id the ID of the entry
   * @param applyOnBackup whether to apply the processor on backup entries
   * @param processor the processor
   * @return the result
   */
  @SuppressWarnings("unchecked")
  static <T> T execute(GlobalID id, boolean applyOnBackup, EntryProcessor<T> processor) {
    try {
      return (T)
          exactlyOnceExecutor.executeOnKey(
              GlobalRuntimeImpl.getRuntime().resilientFinishMap,
              id,
              new AbstractEntryProcessor<GlobalID, ResilientFinishState>(applyOnBackup) {
                private static final long serialVersionUID = -8787905766218374656L;

                @Override
                public T process(Map.Entry<GlobalID, ResilientFinishState> entry) {
                  return processor.process(entry);
                }
              });
    } catch (final DeadPlaceError | HazelcastInstanceNotActiveException e) {
      // this place is dead for the world
      System.out.println(
          "[APGAS] "
              + here()
              + "execute on id="
              + id
              + " DeadPlaceError | HazelcastInstanceNotActiveException");
      System.exit(42);
      throw e;
    }
  }

  /**
   * Apply an entry processor to an entry.
   *
   * @param <T> the return type of the processor
   * @param id the ID of the entry
   * @param processor the processor
   * @return the result
   */
  static <T> T execute(GlobalID id, EntryProcessor<T> processor) {
    return execute(id, true, processor);
  }

  /**
   * Computes the index of the (p, q) counter.
   *
   * @param p source place ID
   * @param q destination place ID
   * @return the computed index
   */
  private static long index(int p, int q) {
    return ((long) p << 32) + q;
  }

  /**
   * Updates the finish states when a place dies.
   *
   * @param p the dead place ID
   */
  static void purge(int p) {
    final int here = GlobalRuntimeImpl.getRuntime().here;
    // only process finish states for the current place and the dead place
    final Predicate<GlobalID, ResilientFinishState> predicate =
        entry -> (entry.getKey().home.id == here || entry.getKey().home.id == p);
    for (final GlobalID id : GlobalRuntimeImpl.getRuntime().resilientFinishMap.keySet(predicate)) {
      submit(
          id,
          state -> {
            if (state == null) {
              // entry has been removed already, ignore
              return null;
            }
            if (state.deads == null) {
              state.deads = new HashSet<>();
            }
            if (state.deads.contains(p)) {
              // death of p has already been processed
              return null;
            }
            state.deads.add(p);
            final int count = state.counts.size();
            for (int i = 0; i <= state.max; i++) {
              state.clear(p, i);
              state.clear(i, p);
            }
            if (state.counts.size() < count) {
              if (state.exceptions == null) {
                state.exceptions = new ArrayList<>();
              }
              state.exceptions.add(new SerializableThrowable(new DeadPlaceException(new Place(p))));
            }
            return state;
          });
    }
  }

  /**
   * Deregisters a listener.
   *
   * @param registration the unique id of the registration
   */
  static void removeListener(String registration) {
    GlobalRuntimeImpl.getRuntime().resilientFinishMap.removeEntryListener(registration);
  }

  /**
   * Updates a resilient finish state asynchronously and propagates termination to parent if
   * necessary.
   *
   * @param id the finish state ID to update
   * @param processor the function to apply
   */
  static void submit(GlobalID id, Processor processor) {
    exactlyOnceExecutor.submitOnKey(
        GlobalRuntimeImpl.getRuntime().resilientFinishMap,
        id,
        new AbstractEntryProcessor<GlobalID, ResilientFinishState>(true) {
          private static final long serialVersionUID = 1754842053698962361L;

          @Override
          public GlobalID process(Map.Entry<GlobalID, ResilientFinishState> entry) {
            final ResilientFinishState state = processor.process(entry.getValue());
            if (state == null) {
              return null;
            }
            if (state.counts.size() > 0
                || state.cids != null && !state.cids.isEmpty()
                || state.deads == null
                || !state.deads.contains(id.home.id)) {
              // state is still useful:
              // finish is incomplete or we need to preserve its exceptions
              entry.setValue(state);
            } else {
              // finish is complete and place of finish has died, remove entry
              entry.setValue(null);
            }
            if (state.counts.size() > 0 || state.cids != null && !state.cids.isEmpty()) {
              return null;
            }
            return state.pid;
          }
        },
        new ExecutionCallback<GlobalID>() {

          @Override
          public void onFailure(Throwable t) {
            if (t instanceof DeadPlaceError || t instanceof HazelcastInstanceNotActiveException) {
              // this place is dead for the world
              System.exit(42);
            }
          }

          @Override
          public void onResponse(GlobalID pid) {
            if (pid == null) {
              return;
            }
            submit(
                pid,
                state -> {
                  if (state == null) {
                    // parent has been purged already
                    // stop propagating termination
                    return null;
                  }
                  if (state.cids != null && state.cids.contains(id)) {
                    state.cids.remove(id);
                  } else {
                    if (state.dids == null) {
                      state.dids = new HashSet<>();
                    }
                    state.dids.add(id);
                  }
                  return state;
                });
          }
        });
  }

  /**
   * Updates a resilient finish state.
   *
   * @param id the finish state ID to update
   * @param processor the function to apply
   */
  static void update(GlobalID id, Processor processor) {
    execute(
        id,
        entry -> {
          entry.setValue(processor.process(entry.getValue()));
          return null;
        });
  }

  /**
   * Update counter by delta
   *
   * @param index the index of the counter
   * @param delta the delta
   */
  private void add(long index, int delta) {
    final int v = counts.getOrDefault(index, 0) + delta;
    if (v == 0) {
      counts.remove(index);
    } else {
      counts.put(index, v);
    }
  }

  public void addCid(GlobalID g) {
    cids.add(g);
  }

  public void addException(SerializableThrowable e) {
    exceptions.add(e);
  }

  /**
   * Clears (p, q) counter.
   *
   * @param p source place ID
   * @param q destination place ID
   */
  void clear(int p, int q) {
    counts.remove(index(p, q));
  }

  /**
   * Decrements (p, q) counter.
   *
   * @param p source place ID
   * @param q destination place ID
   */
  void decr(int p, int q) {
    if (p > max) {
      max = p;
    }
    if (q > max) {
      max = q;
    }
    add(index(p, q), -1);
  }

  public Set<GlobalID> getCids() {
    return cids;
  }

  public void setCids(Set<GlobalID> c) {
    cids = c;
  }

  public Map<Long, Integer> getCounts() {
    return counts;
  }

  public Set<Integer> getDeads() {
    return deads;
  }

  public Set<GlobalID> getDids() {
    return dids;
  }

  public List<SerializableThrowable> getExceptions() {
    return exceptions;
  }

  public void setExceptions(List<SerializableThrowable> e) {
    exceptions = e;
  }

  /**
   * Increments (p, q) counter.
   *
   * @param p source place ID
   * @param q destination place ID
   */
  void incr(int p, int q) {
    if (p > max) {
      max = p;
    }
    if (q > max) {
      max = q;
    }
    add(index(p, q), 1);
  }

  /**
   * An entry processor.
   *
   * @param <T> the return type of the processor
   */
  @FunctionalInterface
  interface EntryProcessor<T> extends Serializable {

    /**
     * The function.
     *
     * @param entry the entry to process
     * @return the result
     */
    T process(Map.Entry<GlobalID, ResilientFinishState> entry);
  }

  /** A function to process finish states. */
  @FunctionalInterface
  interface Processor extends Serializable {

    /**
     * The function.
     *
     * @param state the state to process
     * @return the updated state or null
     */
    ResilientFinishState process(ResilientFinishState state);
  }

  private static class EntryUpdatedOrRemovedListener
      implements EntryUpdatedListener<GlobalID, ResilientFinishState>,
          EntryRemovedListener<GlobalID, ResilientFinishState> {

    private final ResilientFinish finish;

    private EntryUpdatedOrRemovedListener(ResilientFinish finish) {
      this.finish = finish;
    }

    @Override
    public void entryRemoved(EntryEvent<GlobalID, ResilientFinishState> event) {
      synchronized (finish) {
        finish.notifyAll();
      }
    }

    @Override
    public void entryUpdated(EntryEvent<GlobalID, ResilientFinishState> event) {
      synchronized (finish) {
        finish.notifyAll();
      }
    }
  }
}
