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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Presto AccessControl Manager.
 */
public class AccessControlManager {
  private Logger LOG = LoggerFactory.getLogger(PrestoInterpreter.class);
  private static AccessControlManager instance;
  private Map<String, Map<String, Set<Operation>>> grantedOperations =
      new HashMap<String,  Map<String, Set<Operation>>>();
  private Map<String, Set<String>> grantedTables = new HashMap<String,  Set<String>>();
  private Map<String, Map<String, Set<String>>> grantedColumns =
      new HashMap<String,  Map<String, Set<String>>>();

  private Map<String, Set<String>> partitionColumns =
      new HashMap<String, Set<String>>();

  private final Properties interpreterProperties;

  private AccessControlManager(Properties interpreterProperties) {
    this.interpreterProperties = interpreterProperties;
  }

  private Connection conn;

  public static synchronized AccessControlManager getInstance(
      Properties interpreterProperties) throws IOException {
    if (instance == null) {
      instance = new AccessControlManager(interpreterProperties);
      instance.loadConfig();
    }

    return instance;
  }

  public synchronized Set<String> getPartitionColumnsFromPresto(String tableFqn) {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      if (conn == null) {
        URI prestoServer = new URI(interpreterProperties.getProperty(PrestoInterpreter.PRESTOSERVER_URL));

        String prestoJdbcUrl = "jdbc:presto://" + prestoServer.getHost() + ":" + prestoServer.getPort();
        Class.forName("com.facebook.presto.jdbc.PrestoDriver");

        conn = DriverManager.getConnection(prestoJdbcUrl, interpreterProperties.getProperty(PrestoInterpreter.PRESTOSERVER_USER), "");
      }
      stmt = conn.createStatement();
      rs = stmt.executeQuery("SHOW COLUMNS FROM " + tableFqn);

      Set<String> partitionColumns = new HashSet<String>();
      while (rs.next()) {
        String extraValue = rs.getString(3);
        if ("partition key".equals(extraValue)) {
          partitionColumns.add(tableFqn + "." + rs.getString(1));
        }
      }
      return partitionColumns;
    } catch (SQLException e) {
      if (e.getMessage() != null && e.getMessage().indexOf("does not exist") >= 0) {
        return null;
      }
      LOG.error(e.getMessage(), e);
      return null;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
        }
      }

      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
        }
      }
    }
  }

  public Set<String> getPartitionColumns(String tableFqn) {
    if (!tableFqn.startsWith("hive.")) {
      return null;
    }
    synchronized(partitionColumns) {
      Set<String> partitionColumn = partitionColumns.get(tableFqn);
      if (partitionColumn == null) {
        partitionColumn = getPartitionColumnsFromPresto(tableFqn);
        partitionColumns.put(tableFqn, partitionColumn);
      }

      return partitionColumn;
    }
  }

  public synchronized void loadConfig() throws IOException {
    grantedOperations.clear();
    grantedTables.clear();
    grantedColumns.clear();
    partitionColumns.clear();

    Properties properties = new Properties();

    String aclPropertiesFile =
        (String) interpreterProperties.get(PrestoInterpreter.PRESTO_ACL_PROPERTY);

    InputStream in = null;
    if (aclPropertiesFile != null && !aclPropertiesFile.trim().isEmpty()) {
      in = new FileInputStream(aclPropertiesFile);
    } else if (in == null) {
      in = ClassLoader.getSystemResourceAsStream("presto-acl.properties");
      if (in == null) {
        in = ClassLoader.getSystemResourceAsStream("conf/presto-acl.properties");
      }
    }
    if (in != null) {
      properties.load(in);

      Enumeration keys = properties.keys();

      while (keys.hasMoreElements()) {
        Object key = keys.nextElement();

        if (key.toString().startsWith("common.")) {
          continue;
        }

        String permission = properties.get(key).toString();

        String[] keyTokens = key.toString().split("\\.");

        if (keyTokens.length < 4) {
          LOG.warn("Wrong acl property format: " + key.toString());
          continue;
        }
        String resourceType = keyTokens[0];
        String userOrGroup = keyTokens[1];
        String catalog = keyTokens[2];
        String schema = keyTokens[3];
        if ("catalog".equals(resourceType)) {
          addCatalogPermission(userOrGroup, catalog, schema, permission);
        } else if ("table".equals(resourceType)) {
          addTablePermission(userOrGroup, catalog, schema, permission);
        } else if ("column".equals(resourceType)) {
          String table = keyTokens[4];
          addColumnPermission(userOrGroup, catalog, schema, table, permission);
        } else {
          LOG.warn("Wrong acl property format: " + key.toString());
        }
      }
    } else {
      LOG.error("No presto-acl.properties files in classpath.");
    }
  }

  private void addColumnPermission(String userOrGroup, String catalog, String schema, String tableName, String permission) {
    if ("*".equals(permission)) {
      return;
    } else {
      String tableFqn = catalog + "." + schema + "." + tableName;

      String[] columns = permission.split(",");
      for (String column : columns) {
        Map<String, Set<String>> userGrantedColumns = grantedColumns.get(userOrGroup);
        if (userGrantedColumns == null) {
          userGrantedColumns = new HashMap<String, Set<String>>();
          grantedColumns.put(userOrGroup, userGrantedColumns);
        }

        Set<String> tableColumns = userGrantedColumns.get(tableFqn);
        if (tableColumns == null) {
          tableColumns = new HashSet<String>();
          userGrantedColumns.put(tableFqn, tableColumns);
        }

        //LOG.info("Add column permission: " + userOrGroup + "," + tableFqn + "." + column);
        tableColumns.add(column);
      }
    }
  }

  private void addTablePermission(String userOrGroup, String catalog, String schema, String permission) {
    if ("*".equals(permission)) {
      String tableFqn = catalog + "." + schema + "." + permission;
      Set<String> userGrantedTables = grantedTables.get(userOrGroup);
      if (userGrantedTables == null) {
        userGrantedTables = new HashSet<String>();
        grantedTables.put(userOrGroup, userGrantedTables);
      }
      userGrantedTables.add(tableFqn);
    } else {
      String[] tables = permission.split(",");
      for (String table : tables) {
        String tableFqn = catalog + "." + schema + "." + table;
        Set<String> userGrantedTables = grantedTables.get(userOrGroup);
        if (userGrantedTables == null) {
          userGrantedTables = new HashSet<String>();
          grantedTables.put(userOrGroup, userGrantedTables);
        }
        //LOG.info("Add table permission: " + userOrGroup + "," + tableFqn);
        userGrantedTables.add(tableFqn);
      }
    }
  }

  private void addCatalogPermission(String userOrGroup, String catalog, String schema, String permission) {
    String[] permissionToken = permission.split(",");
    String schemaFqn = catalog + "." + schema;
    for (String eachPermission:permissionToken) {
      Map<String, Set<Operation>> userGrantedOperations = grantedOperations.get(userOrGroup);
      if (userGrantedOperations == null) {
        userGrantedOperations = new HashMap<String, Set<Operation>>();
        grantedOperations.put(userOrGroup, userGrantedOperations);
      }

      Set<Operation> schemaOperations = userGrantedOperations.get(schemaFqn);
      if (schemaOperations == null) {
        schemaOperations = new HashSet<Operation>();
        userGrantedOperations.put(schemaFqn, schemaOperations);
      }

      //LOG.info("Add catalog permission: " + userOrGroup + "," + eachPermission);
      schemaOperations.add(Operation.getOperation(eachPermission));
    }
  }


  /**
   * AclResult.
   */
  public enum AclResult {
    OK, DENY, NEED_PARTITION_COLUMN
  }

  public AclResult checkAcl(String sql, String queryPlan, String principal,
                            StringBuilder errorMessage) throws IOException {
    if (!"true".equals(interpreterProperties.get(PrestoInterpreter.PRESTO_ACL_ENABLE))) {
      return AclResult.OK;
    }
    boolean isInfoQuery = sql.toLowerCase().trim().startsWith("show");
    boolean isDescQuery = sql.toLowerCase().trim().startsWith("desc");

    //System.out.println(">>>>>>" + queryPlan);
    BufferedReader reader = new BufferedReader(new StringReader(queryPlan));

    try {
      String line = null;
      line = reader.readLine();
      while (true) {
        if (line == null) {
          break;
        }
        line = line.trim();
        boolean meaningfulLine = false;
        if (line.startsWith("-")) {
          line = line.substring(1).trim();
          meaningfulLine = true;
        } else if (line.startsWith("DROP")) {
          meaningfulLine = true;
        }

        if (line.startsWith("This connector does not support")) {
          errorMessage.append(line);
          return AclResult.DENY;
        }

        StringBuilder lastLine = new StringBuilder();
        if (meaningfulLine) {
          Operation requiredPermission = null;
          List<String> aclTargetResources = null;
          boolean isAclTarget = true;
          if (line.startsWith("TableCommit")) {
            String schema = getSchemaFromTableCommitPlan(line);
            if (schema != null) {
              aclTargetResources = new ArrayList<String>();
              aclTargetResources.add(schema);
            }
            requiredPermission = Operation.CREATE;
          } else if (!isInfoQuery && isScanOperator(line)) {
            AtomicBoolean hasPartitionKey = new AtomicBoolean(true);
            StringBuilder noPartitionColumnInfo = new StringBuilder();
            aclTargetResources = parseTableScanPlan(reader, line, isDescQuery,
                lastLine, hasPartitionKey, noPartitionColumnInfo);
            if (!hasPartitionKey.get()) {
              errorMessage.append(
                  "No partition column(" + noPartitionColumnInfo + ") in where clause.");
              return AclResult.NEED_PARTITION_COLUMN;
            }
            requiredPermission = Operation.READ;
          } else if (line.startsWith("DROP")) {
            aclTargetResources = parseDropPlan(line);
            requiredPermission = Operation.DROP;
          } else {
            isAclTarget = false;
          }

          if (isAclTarget) {
            if (aclTargetResources == null) {
              errorMessage.append("Can't parse execution plan");
              LOG.error("Can't parse execution plan: " + queryPlan);
              return AclResult.DENY;
            }
            for (String eachResource : aclTargetResources) {
              if (!canAccess(principal, eachResource, requiredPermission)) {
                errorMessage.append(
                    "Can't access " + eachResource.toString() + " for " + requiredPermission);
                return AclResult.DENY;
              }
            }
          }
        }

        if (lastLine.length() > 0) {
          line = lastLine.toString();
        } else {
          line = reader.readLine();
        }
      }
      return AclResult.OK;
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Can't parse execution plan: " + e.getMessage() + "\n" + queryPlan, e);
      errorMessage.append("Error while parsing execution plan: " + e.getMessage());
      return AclResult.DENY;
    } finally {
      reader.close();
    }
  }

  protected boolean isScanOperator(String line) {
    return line.startsWith("ScanFilterProject") ||
        line.startsWith("ScanFilter") ||
        line.startsWith("TableScan") ||
        line.startsWith("ScanProject");
  }

  public String getSchemaFromTableCommitPlan(String currentLine) {
    int startPos = currentLine.indexOf("}:");
    if (startPos <= 0) {
      return null;
    }
    currentLine = currentLine.substring(startPos + 2);
    currentLine = currentLine.substring(0, currentLine.indexOf("]"));

    String[] tokens = currentLine.split(":");
    String catalog = tokens[0];

    String[] schemaTableTokens = tokens[1].split("\\.");
    String schema = schemaTableTokens[0];

    //LOG.debug("Parsed: " + catalog + "." + schema);
    return catalog + "." + schema;
  }

  public List<String> parseTableScanPlan(
      BufferedReader reader,
      String currentLine,
      boolean isDescQuery,
      StringBuilder lastReadLine,
      AtomicBoolean hasPartitionKey,
      StringBuilder noPartitionColumnInfo) throws IOException {

    if (isDescQuery) {
      String catalog = currentLine.substring(currentLine.indexOf("$info_schema@") + "$info_schema@".length(), currentLine.indexOf(":"));
      String line = currentLine.substring(currentLine.indexOf("filterPredicate = ((\"table_schema\" = CAST('") + "filterPredicate = ((\"table_schema\" = CAST('".length());
      String schema = line.substring(0, line.indexOf("' AS varchar"));
      line = line.substring(line.indexOf("\"table_name\" = CAST('") + "\"table_name\" = CAST('".length());
      String table = line.substring(0, line.indexOf("' AS varchar"));
      List<String> resources = new ArrayList<String>();
      resources.add(catalog + "." + schema + "." + table);
      LOG.debug("Parsed: " + catalog + "." + schema + "." + table);
      return resources;
    }

    String line = null;
    if (currentLine.startsWith("ScanFilterProject[table = ")) {
      line = currentLine.substring("ScanFilterProject[table = ".length());
    } else if (currentLine.startsWith("ScanFilter[table = ")) {
      line = currentLine.substring("ScanFilter[table = ".length());
    } else if (currentLine.startsWith("ScanProject[table = ")) {
      line = currentLine.substring("ScanProject[table = ".length());
    } else if  (currentLine.startsWith("TableScan[")) {
      line = currentLine.substring("TableScan[".length());
    }

    String[] tokens = line.substring(0, line.indexOf(",")).split(":");
    String catalog = null;
    String schema = null;
    String table = null;

    if (tokens.length == 2) {
      catalog = tokens[0];
      String[] subTokens = tokens[1].split("\\.");
      if (subTokens.length == 2) {
        schema = subTokens[0];
        table = subTokens[1];
      }
    } else if (tokens.length == 3) {
      catalog = tokens[0];
//      String[] subTokens = tokens[2].split("\\.");
//      if (subTokens.length == 2) {
//        schema = subTokens[0];
//        table = subTokens[1];
//      }
      schema = tokens[1];
      table = tokens[2];
    } else if (tokens.length > 3) {
      catalog = tokens[1];
      schema = tokens[2];
      table = tokens[3];
    } else {
      throw new IOException("Can't parse catalog, schema, table from [" + currentLine + "]");
    }
    schema = schema.toLowerCase(Locale.ENGLISH).replace('-', '_');
    table = table.toLowerCase(Locale.ENGLISH).replace('-', '_');

    if (table == null) {
      throw new IOException("Can't get table name from [" + currentLine + "]");
    }
    // <Presto-0.168>
//    List<String> resources = new ArrayList<String>();
//    int columnIndex = line.lastIndexOf("=> [");
//    String[] columnTokens = line.substring(columnIndex + 4, line.getBytes().length - 1).split(",");
//    for (String columnToken: columnTokens) {
//      String[] columnInfo = columnToken.trim().split(":");
//      String columnName = columnInfo[0];
//      LOG.debug("Parsed: " + catalog + "." + schema + "." + table + "." + columnName.trim());
//      resources.add(catalog + "." + schema + "." + table + "." + columnName.trim());
//    }
//    return resources;
    // </Presto-0.168>

    List<String> resources = new ArrayList<String>();
    String columnInfoLine = null;
    while ( (columnInfoLine = reader.readLine()) != null ) {
      columnInfoLine = columnInfoLine.trim();
      if (columnInfoLine.trim().startsWith("-")) {
        lastReadLine.append(columnInfoLine);
        return resources;
      }
      if (columnInfoLine.indexOf("ColumnHandle{name=") < 0) {
        continue;
      }

      int index = columnInfoLine.indexOf("ColumnHandle{name=");
      columnInfoLine = columnInfoLine.substring(index + "ColumnHandle{name=".length());
      String[] columnInfo = columnInfoLine.trim().split(",");
      String columnName = columnInfo[0];
      LOG.info("Parsed: " + catalog + "." + schema + "." + table + "." + columnName.trim());
      resources.add(catalog + "." + schema + "." + table + "." + columnName.trim());
    }

    return resources;
  }

  private List<String> parseDropPlan(String currentLine) {
    int pos = currentLine.indexOf("DROP TABLE ");
    if (pos < 0) {
      return null;
    }

    String[] tokens = currentLine.substring(pos + "DROP TABLE ".length()).trim().split("\\.");
    List<String> resources = new ArrayList<String>();
    resources.add(tokens[0] + "." + tokens[1] + "." + tokens[2]);

    LOG.info("Parsed: " + tokens[0] + "." + tokens[1] + "." + tokens[2]);
    return resources;
  }

  public boolean canAccess(String principal,
                           String resource,
                           Operation permissionType) {
    if (grantedOperations.isEmpty()) {
      LOG.warn("No ACL properties. Please check your presto-acl.properties file");
      return true;
    }

    LOG.info("Can Access? " + resource);
    String[] resourceTokens = resource.split("\\.");
    if (resourceTokens.length < 2) {
      LOG.error("Wrong resource format: " + resource);
      return false;
    }
    String catalog = resourceTokens[0];
    String schema = resourceTokens[1];
    String table = resourceTokens.length > 2 ? resourceTokens[2] : null;
    String column = resourceTokens.length > 3 ? resourceTokens[3] : null;

    Map<String, Set<Operation>> userAcls = grantedOperations.get(principal);
    if (userAcls == null) {
      return false;
    }

    // Check Catalog and Schema
    String schemaFqn = catalog + "." + schema;
    Set<Operation> permissions = userAcls.get(schemaFqn);
    if (permissions == null || !permissions.contains(permissionType)) {
      permissions = userAcls.get(catalog + ".*");
      if (permissions == null || !permissions.contains(permissionType)) {
        return false;
      }
    }

    // Check Table
    if (table == null) {
      return true;
    }

    Set<String> userGrantedTables = grantedTables.get(principal);
    if (userGrantedTables == null) {
      return false;
    }
    String tableFqn = schemaFqn + "." + table;
    if (!userGrantedTables.contains(tableFqn)) {
      if (!userGrantedTables.contains(schemaFqn + ".*") && !userGrantedTables.contains(catalog + ".*.*")) {
        return false;
      }
    }

    if (column == null) {
      return true;
    }

    Map<String, Set<String>> userGrantedColumns = grantedColumns.get(principal);
    if (userGrantedColumns == null) {
      return true;
    }
    Set<String> tableColumns = userGrantedColumns.get(tableFqn);
    if (tableColumns == null) {
      return true;
    }

    return tableColumns.contains(column);
  }
}
