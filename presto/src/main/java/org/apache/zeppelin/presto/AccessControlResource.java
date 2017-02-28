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
 * Presto Access Control Resource.
 */
public class AccessControlResource {
  private String catalog;
  private String schema;
  private String table;
  private String column;

  public AccessControlResource(String catalog,
                               String schema,
                               String table,
                               String column) {
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.column = column;
  }

  public String toString() {
    String result = catalog + "." + schema;
    if (table != null) {
      result += "." + table;
    }

    if (column != null) {
      result += "." + column;
    }

    return result;
  }

  public int hashCode() {
    return toString().hashCode();
  }

  public boolean equals(Object o) {
    if (!(o instanceof AccessControlResource)) {
      return false;
    }

    return toString().equals(o.toString());
  }

  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }
}
