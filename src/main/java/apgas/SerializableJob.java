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

package apgas;

import java.io.Serializable;

/**
 * A serializable functional interface with no arguments and no return value.
 *
 * <p>The functional method is {@link #run()}.
 */
@FunctionalInterface
public interface SerializableJob extends Serializable, Job {

  @Override
  void run() throws Exception;
}
