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
import apgas.SerializableJob;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

/**
 * The {@link Task} class represents an APGAS task.
 *
 * <p>This class implements task serialization and handles errors in the serialization process.
 */
public final class Task extends RecursiveAction implements SerializableRunnable {

  private static final SerializableJob NULL = () -> {};

  private static final long serialVersionUID = 5288338719050788305L;

  /** The finish object for this {@link Task} instance. */
  Finish finish;

  /** The function to run. */
  private SerializableJob f;

  /** The place of the parent task. */
  private int parent;

  /**
   * Constructs a new {@link Task}.
   *
   * @param f the function to run
   * @param finish the finish object for this task
   * @param parent the place of the parent task
   */
  Task(Finish finish, SerializableJob f, int parent) {
    this.finish = finish;
    this.f = f;
    this.parent = parent;
  }

  /**
   * Submits the task for asynchronous execution.
   *
   * @param worker the worker doing the submission or null if not a worker thread
   */
  void async(Worker worker) {
    finish.submit(parent);

    if (worker == null) {
      GlobalRuntimeImpl.getRuntime().execute(this);
    } else {
      fork();
    }
  }

  /**
   * Submits the task for asynchronous execution at place p.
   *
   * <p>If serialization fails, the task is dropped and the task's finish is notified. The exception
   * is logged to System.err and masked unless APGAS_SERIALIZATION_EXCEPTION is set to "true".
   *
   * @param p the place ID
   */
  void asyncAt(int p) {
    try {
      GlobalRuntimeImpl.getRuntime().transport.send(p, this);
    } catch (final Throwable e) {
      finish.unspawn(p);
      throw e;
    }
  }

  /** Runs the task and notify the task's finish upon termination. */
  @Override
  protected void compute() {
    final Worker worker = (Worker) Thread.currentThread();
    worker.task = this;
    try {
      f.run();
    } catch (final Throwable t) {
      System.out.println("[APGAS] " + here() + " caught Exception");
      t.printStackTrace(System.out);
      finish.addSuppressed(t);
    }
    finish.tell();
  }

  /**
   * Runs the tasks, notify the task's finish upon termination, and wait for the task's finish to
   * terminate.
   *
   * @param worker the worker thread running the task or null if not a worker thread
   */
  void finish(Worker worker) {
    if (worker == null) {
      async(null);
      try {
        ForkJoinPool.managedBlock(finish);
      } catch (final InterruptedException e) {
      }
    } else {
      final Task savedTask = worker.task;
      compute();
      Task t;
      while (!finish.isReleasable()
          && (t = (Task) ForkJoinTask.peekNextLocalTask()) != null
          && finish == t.finish
          && t.tryUnfork()) {
        t.compute();
      }
      try {
        ForkJoinPool.managedBlock(finish);
      } catch (final InterruptedException e) {
      }
      worker.task = savedTask;
    }
  }

  /**
   * Returns the finish managing this task
   *
   * @return the finish to which this task belongs to
   */
  public Finish getFinish() {
    return finish;
  }

  /**
   * Deserializes the task.
   *
   * <p>If deserialization fails, a dummy task is returned. The exception is logged to System.err
   * and masked unless APGAS_SERIALIZATION_EXCEPTION is set to "true".
   *
   * @param in the object input stream
   * @throws IOException if I/O errors occur
   * @throws ClassNotFoundException if the class of the serialized object cannot be found
   */
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    finish = (Finish) in.readObject();
    parent = in.readInt();
    try {
      f = (SerializableJob) in.readObject();
    } catch (final Throwable e) {
      finish.addSuppressed(e);
      f = NULL;
    }
  }

  /** Submits the task for asynchronous execution. */
  @Override
  public void run() {
    try {
      async(null);
    } catch (final DeadPlaceException e) {
      // source place has died while task was in transit, discard
    }
  }

  /**
   * Serializes the task.
   *
   * @param out the object output stream
   * @throws IOException if I/O errors occur
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeObject(finish);
    out.writeInt(parent);
    out.writeObject(f);
  }
}
