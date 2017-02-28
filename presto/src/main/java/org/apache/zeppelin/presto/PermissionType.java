/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.zeppelin.presto;

/**
 * Presto PermissionType.
 */
public enum PermissionType {
  READ("read"), WRITE("write"), DENY("deny"), ALLOW("allow");

  private String name;
  PermissionType(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

  static PermissionType getPermissionType(String name) {
    if (name.equals(READ.name)) {
      return READ;
    } else if (name.equals(WRITE.name)) {
      return WRITE;
    } else if (name.equals(DENY.name)) {
      return DENY;
    } else if (name.equals(ALLOW.name)) {
      return ALLOW;
    } else {
      return null;
    }
  }
}
