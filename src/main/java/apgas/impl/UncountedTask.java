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

import apgas.SerializableJob;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.RecursiveAction;

/**
 * The {@link UncountedTask} class represents an uncounted task.
 *
 * <p>This class implements task serialization and handles errors in the serialization process.
 */
final class UncountedTask extends RecursiveAction implements SerializableRunnable {

  private static final SerializableJob NULL = () -> {};
  private static final long serialVersionUID = 5031683857632950143L;

  /** The function to run. */
  private SerializableJob f;

  /**
   * Constructs a new {@link UncountedTask}.
   *
   * @param f the function to run
   */
  UncountedTask(SerializableJob f) {
    this.f = f;
  }

  @Override
  protected void compute() {
    try {
      final Worker worker = (Worker) Thread.currentThread();
      worker.task = null;
      f.run();
    } catch (final Throwable t) {
      System.err.println("[APGAS] Uncaught exception in uncounted task");
      System.err.println("[APGAS] Caused by: " + t);
      System.err.println("[APGAS] Ignoring...");
      t.printStackTrace();
    }
  }

  /**
   * Deserializes the task.
   *
   * <p>If deserialization fails, the task is dropped. The exception is logged to System.err.
   *
   * @param in the object input stream
   * @throws IOException if I/O errors occur
   * @throws ClassNotFoundException if the class of the serialized object cannot be found
   */
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    try {
      f = (SerializableJob) in.readObject();
    } catch (final Throwable e) {
      System.err.println(
          "[APGAS] Ignoring failure to receive an uncounted task at place "
              + GlobalRuntimeImpl.getRuntime().here
              + " due to: "
              + e);
      f = NULL;
    }
  }

  /** Submits the task for asynchronous execution. */
  @Override
  public void run() {
    GlobalRuntimeImpl.getRuntime().execute(this);
  }

  /**
   * Submits the task for asynchronous uncounted execution at place p.
   *
   * <p>If serialization fails, the task is dropped. The exception is logged to System.err and
   * masked unless APGAS_SERIALIZATION_EXCEPTION is set to "true".
   *
   * @param p the place ID
   */
  void uncountedAsyncAt(int p) {
    try {
      GlobalRuntimeImpl.getRuntime().transport.send(p, this);
    } catch (final Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}
