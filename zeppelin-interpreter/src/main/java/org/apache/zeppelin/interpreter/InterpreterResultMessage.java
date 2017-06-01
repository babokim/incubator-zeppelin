/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.interpreter;

import java.util.LinkedList;
import java.util.List;

/**
 * Interpreter result message
 */
public class InterpreterResultMessage {
  InterpreterResult.Type type;
  String data;
  List<LinkedParameter> linkedParameters = new LinkedList<>();

  public InterpreterResultMessage(InterpreterResult.Type type, String data) {
    this.type = type;
    this.data = data;
  }

  public InterpreterResultMessage(InterpreterResult.Type type, String data,
                                  List<LinkedParameter> linkedParameters) {
    this.type = type;
    this.data = data;
    this.linkedParameters = linkedParameters;
  }

  public InterpreterResult.Type getType() {
    return type;
  }

  public String getData() {
    return data;
  }

  public List<LinkedParameter> getLinkedParameters() {
    return linkedParameters;
  }

  public void addLinkParameter(LinkedParameter lp) {
    boolean replaceFlag = false;

    for (int i = 0; i < this.linkedParameters.size(); i++) {
      LinkedParameter params = this.linkedParameters.get(i);

      if (params.getSourceParagraphId().equals(lp.getSourceParagraphId()) &&
              params.getSourceParagraphLinkColumnIdx() == lp.getSourceParagraphLinkColumnIdx()) {
        this.linkedParameters.set(i, lp);
        replaceFlag = true;
        break;
      }
    }

    if (replaceFlag == false) {
      this.linkedParameters.add(lp);
    }
  }

  public String toString() {
    return "%" + type.name().toLowerCase() + " " + data;
  }
}
