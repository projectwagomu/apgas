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

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * The {@link SerializableThrowable} handles exception serialization failures.
 *
 * <p>It replaces non-serializable exceptions with {@link java.io.NotSerializableException}
 * exceptions.
 */
final class SerializableThrowable implements Serializable {

  private static final long serialVersionUID = -3821385333142091656L;

  /** The wrapped exception. */
  Throwable t;

  /**
   * Constructs a new {@link SerializableThrowable}.
   *
   * @param t the exception to wrap
   */
  SerializableThrowable(Throwable t) {
    this.t = t;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    t = (Throwable) in.readObject();
    try {
      t = (Throwable) in.readObject();
    } catch (final Throwable x) {
    }
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    final NotSerializableException e =
        new NotSerializableException(t.getClass().getCanonicalName());
    e.setStackTrace(t.getStackTrace());
    out.writeObject(e);
    try {
      out.writeObject(t);
    } catch (final Throwable x) {
    }
  }
}
