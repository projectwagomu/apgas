/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 */

package apgas.impl;

import java.io.IOException;
import java.io.ObjectInputStream;

import com.hazelcast.core.Member;

/**
 * The {@link ImmediateTask} class represents an immediate task.
 *
 * <p>
 * This class implements task serialization and handles errors in the
 * serialization process.
 */
final class ImmediateTask implements SerializableRunnable {

	private static final SerializableRunnable NULL = () -> {
	};

	private static final long serialVersionUID = -7419887249105833691L;
	/** The function to run. */
	private SerializableRunnable f;

	/**
	 * Constructs a new {@link ImmediateTask}.
	 *
	 * @param f the function to run
	 */
	ImmediateTask(SerializableRunnable f) {
		this.f = f;
	}

	/**
	 * Submits the task for asynchronous uncounted execution at place p.
	 *
	 * <p>
	 * If serialization fails, the task is dropped. The exception is logged to
	 * System.err and masked unless APGAS_SERIALIZATION_EXCEPTION is set to "true".
	 *
	 * @param p the place ID
	 */
	void immediateAsyncAt(int p) {
		try {
			GlobalRuntimeImpl.getRuntime().transport.send(p, this);
		} catch (final Throwable e) {
			throw e;
		}
	}

	/**
	 * Submits the task for asynchronous uncounted execution at place p.
	 *
	 * <p>
	 * If serialization fails, the task is dropped. The exception is logged to
	 * System.err and masked unless APGAS_SERIALIZATION_EXCEPTION is set to "true".
	 *
	 * @param member the hazelcast member
	 */
	void immediateAsyncAt(Member member) {
		try {
			GlobalRuntimeImpl.getRuntime().transport.send(member, this);
		} catch (final Throwable e) {
			throw e;
		}
	}

	/**
	 * Deserializes the task.
	 *
	 * <p>
	 * If deserialization fails, the task is dropped. The exception is logged to
	 * System.err.
	 *
	 * @param in the object input stream
	 * @throws IOException            if I/O errors occur
	 * @throws ClassNotFoundException if the class of the serialized object cannot
	 *                                be found
	 */
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		try {
			f = (SerializableRunnable) in.readObject();
		} catch (final Throwable e) {
			System.err.println("[APGAS] Ignoring failure to receive an uncounted task at place "
					+ GlobalRuntimeImpl.getRuntime().here + " due to: " + e);
			f = NULL;
		}
	}

	/** Submits the task for asynchronous execution. */
	@Override
	public void run() {
		GlobalRuntimeImpl.getRuntime().executeImmediate(f);
	}
}
