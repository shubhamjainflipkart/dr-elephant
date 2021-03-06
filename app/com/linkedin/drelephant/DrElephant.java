/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant;

import java.io.IOException;


/**
 * The main class which starts Dr. Elephant
 */
public class DrElephant extends Thread {
  private ElephantRunner _elephant;

  public DrElephant() throws IOException {
    _elephant = ElephantRunner.getInstance();
  }

  @Override
  public void run() {
    _elephant.run();
  }

  public void kill() {
    if (_elephant != null) {
      _elephant.kill();
    }
  }
}
