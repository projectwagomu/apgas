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

import static apgas.Constructs.here;

import apgas.SerializableCallable;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import java.io.Serializable;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class ExactlyOnceExecutor<T, V extends IncrementalEntryValue> implements Serializable {

  private static final long serialVersionUID = -2650051985799990110L;
  private static final AtomicInteger uid = new AtomicInteger(0);

  private void executeOnce(final long uid, final Entry<T, V> entry, final Runnable runnable) {
    executeOnce(
        uid,
        entry,
        () -> {
          runnable.run();
          return null;
        });
  }

  private Object executeOnce(
      final long uid, final Entry<T, V> entry, final SerializableCallable<Object> callable) {
    V value = entry.getValue();
    if (value != null && value.uid == uid) {
      return null;
    }
    Object ret = null;
    try {
      ret = callable.call();
    } catch (final Exception e) {
      e.printStackTrace();
    }
    value = entry.getValue();
    if (value != null) {
      value.uid = uid;
      entry.setValue(value);
    }

    return ret;
  }

  public Object executeOnKey(
      final IMap<T, V> map, final T key, final EntryProcessor<T, V> processor) {
    final long uid = ExactlyOnceExecutor.uid.incrementAndGet() + ((long) here().id << 32);
    final EntryBackupProcessor<T, V> backupProcessor = processor.getBackupProcessor();
    Object ret = null;
    try {
      ret =
          map.executeOnKey(
              key,
              new EntryProcessor<T, V>() {
                private static final long serialVersionUID = 8975580950514264082L;

                @Override
                public EntryBackupProcessor<T, V> getBackupProcessor() {
                  if (backupProcessor == null) {
                    return null;
                  }
                  return entry -> {
                    executeOnce(
                        uid,
                        entry,
                        () -> {
                          backupProcessor.processBackup(entry);
                        });
                  };
                }

                @Override
                public Object process(Entry<T, V> entry) {
                  return executeOnce(uid, entry, () -> processor.process(entry));
                }
              });
    } catch (final Throwable t) {
      System.out.println("EOE Throwable " + t);
      t.printStackTrace(System.out);
    }
    return ret;
  }

  public Future<?> submitOnKey(IMap<T, V> map, T key, final EntryProcessor<T, V> processor) {
    final long uid = ExactlyOnceExecutor.uid.incrementAndGet() + ((long) here().id << 32);
    final EntryBackupProcessor<T, V> backupProcessor = processor.getBackupProcessor();
    return map.submitToKey(
        key,
        new EntryProcessor<T, V>() {
          private static final long serialVersionUID = -2225168921089863027L;

          @Override
          public EntryBackupProcessor<T, V> getBackupProcessor() {
            if (backupProcessor == null) {
              return null;
            }
            return entry -> {
              executeOnce(
                  uid,
                  entry,
                  () -> {
                    backupProcessor.processBackup(entry);
                  });
            };
          }

          @Override
          public Object process(Entry<T, V> entry) {
            return executeOnce(uid, entry, () -> processor.process(entry));
          }
        });
  }

  public void submitOnKey(
      IMap<T, V> map,
      T key,
      final EntryProcessor<T, V> processor,
      final ExecutionCallback<T> callback) {
    final long uid = ExactlyOnceExecutor.uid.incrementAndGet() + ((long) here().id << 32);
    final EntryBackupProcessor<T, V> backupProcessor = processor.getBackupProcessor();
    map.submitToKey(
        key,
        new EntryProcessor<T, V>() {
          private static final long serialVersionUID = -2225168921089863027L;

          @Override
          public EntryBackupProcessor<T, V> getBackupProcessor() {
            if (backupProcessor == null) {
              return null;
            }
            return entry -> {
              executeOnce(
                  uid,
                  entry,
                  () -> {
                    backupProcessor.processBackup(entry);
                  });
            };
          }

          @Override
          public Object process(Entry<T, V> entry) {
            return executeOnce(uid, entry, () -> processor.process(entry));
          }
        },
        new ExecutionCallback<T>() {
          @Override
          public void onFailure(Throwable t) {
            t.printStackTrace(System.out);
            callback.onFailure(t);
          }

          @Override
          public void onResponse(T arg0) {
            callback.onResponse(arg0);
          }
        });
  }
}
