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

import java.io.Serializable;

public class IncrementalEntryValue implements Serializable {

  private static final long serialVersionUID = -8035830523372798040L;

  public long uid;

  public IncrementalEntryValue() {
    uid = -1L;
  }
}
