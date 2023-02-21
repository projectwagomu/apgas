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

package apgas.launcher;

import java.io.IOException;
import java.util.List;

/** The {@link SrunLauncher} class implements a launcher using srun. */
public class SrunLauncher extends RemoteLauncher {

  @Override
  void startRemote(List<String> command, boolean verbose, String hostAddress) {
    command.add(0, "srun");
    command.add(1, "--mem=180G");
    command.add(2, "-N 1");
    command.add(3, "-n 1");
    command.add(4, "-w");
    command.add(5, hostAddress);
    if (verbose) {
      System.err.println("[APGAS] Spawning new place: " + String.join(" ", command));
    }
    try {
      super.process = super.processBuilder.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
    command.remove(0);
    command.remove(0);
    command.remove(0);
    command.remove(0);
    command.remove(0);
    command.remove(0);
  }
}
