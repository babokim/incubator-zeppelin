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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Presto AccessControl Manager.
 */
public class AccessControlManager {
  public static final Log LOG = LogFactory.getLog(AccessControlManager.class);
  private static AccessControlManager instance;
  private Map<String, Map<AccessControlResource, Set<PermissionType>>> acls =
      new HashMap<String,  Map<AccessControlResource, Set<PermissionType>>>();
  private Map<String, HashSet<AccessControlResource>> tablesHasDeniedColumns =
      new HashMap<String, HashSet<AccessControlResource>>();
  private Map<String, HashSet<AccessControlResource>> tablesHasAllowedColumns =
      new HashMap<String, HashSet<AccessControlResource>>();

  private final Properties interpreterProperties;

  private Set<String> hiveCatalogNames = new HashSet<String>();

  //catalog.schema.table -> column name
  private Map<String, String> partitionColumnNames = new HashMap<String, String>();

  private AccessControlManager(Properties interpreterProperties) {
    this.interpreterProperties = interpreterProperties;
  }

  public static synchronized AccessControlManager getInstance(
      Properties interpreterProperties) throws IOException {
    if (instance == null) {
      instance = new AccessControlManager(interpreterProperties);
      instance.loadConfig();
    }

    return instance;
  }

  public synchronized void loadConfig() throws IOException {
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

      // Common property
      Object hiveCatalogs = properties.get("common.hive.metastore.catalogs");
      if (hiveCatalogs != null) {
        for (String eachHiveCatalog: hiveCatalogs.toString().split(",")) {
          hiveCatalogNames.add(eachHiveCatalog.trim());
        }
      }

      Object partitionColumns = properties.get("common.partitioned.columns");
      if (partitionColumns != null) {
        for (String eachPartitionColumn: partitionColumns.toString().split(",")) {
          String[] tokens = eachPartitionColumn.trim().split("\\.");
          if (tokens.length != 4) {
            LOG.warn("Wrong common.partitioned.columns format. " +
                "Format should be <catalog>.<schema>.<table>.<column>");
            continue;
          }
          partitionColumnNames.put(tokens[0] + "." + tokens[1] + "." + tokens[2], tokens[3]);
        }
      }

      // ACL properties
      Enumeration keys = properties.keys();

      while (keys.hasMoreElements()) {
        Object key = keys.nextElement();

        if (key.toString().startsWith("common.")) {
          continue;
        }

        String permission = properties.get(key).toString();

        String[] keyTokens = key.toString().split("\\.");

        if (keyTokens.length < 3) {
          LOG.warn("Wrong acl property format: " + key.toString());
          continue;
        }
        String userOrGroup = keyTokens[0];
        String catalog = keyTokens[1];
        String schema = keyTokens[2];
        String table = keyTokens.length > 3 ? keyTokens[3] : null;
        String column = keyTokens.length > 4 ? keyTokens[4] : null;
        String[] columns = column != null ? column.split(",") : null;

        String[] permissionTokens = permission.split(",");

        HashSet<PermissionType> permissions = new HashSet<PermissionType>();
        for (String eachToken: permissionTokens) {
          permissions.add(PermissionType.getPermissionType(eachToken.trim()));
        }

        Map<AccessControlResource, Set<PermissionType>> userAcl = acls.get(userOrGroup);
        if (userAcl == null) {
          userAcl = new HashMap<AccessControlResource, Set<PermissionType>>();
          acls.put(userOrGroup, userAcl);
        }

        AccessControlResource tableResource =
            new AccessControlResource(catalog, schema, table, null);
        if (columns == null) {
          AccessControlResource resource =
              new AccessControlResource(catalog, schema, table, column);
          if (userAcl.containsKey(resource)) {
            throw new IOException("acl.properties has conflicts: " + resource);
          }
          userAcl.put(resource, permissions);
        } else {
          for (String eachColumn: columns) {
            AccessControlResource resource =
                new AccessControlResource(catalog, schema, table,
                    eachColumn.trim());
            if (userAcl.containsKey(resource)) {
              throw new IOException("acl.properties has conflicts: " + resource);
            }
            userAcl.put(resource, permissions);


            if (!eachColumn.equals("*")) {
              if (permissions.contains(PermissionType.ALLOW)) {
                HashSet<AccessControlResource> allowedColumnTables =
                    tablesHasAllowedColumns.get(userOrGroup);
                if (allowedColumnTables == null) {
                  allowedColumnTables = new HashSet<AccessControlResource>();
                  tablesHasAllowedColumns.put(userOrGroup, allowedColumnTables);
                }
                allowedColumnTables.add(tableResource);
              } else {
                HashSet<AccessControlResource> deniedColumnTables =
                    tablesHasDeniedColumns.get(userOrGroup);
                if (deniedColumnTables == null) {
                  deniedColumnTables = new HashSet<AccessControlResource>();
                  tablesHasDeniedColumns.put(userOrGroup, deniedColumnTables);
                }
                deniedColumnTables.add(tableResource);
              }
            }
          }
        }
      }
    } else {
      LOG.error("No presto-acl.properties files in classpath.");
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

    boolean isInfoQuery = sql.toLowerCase().trim().startsWith("desc") ||
        sql.toLowerCase().trim().startsWith("show tables");

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
          PermissionType requiredPermission = null;
          List<AccessControlResource> resources = null;
          boolean isAclTarget = true;
          if (line.startsWith("TableCommit")) {
            resources = parseTableCommitPlan(line);
            requiredPermission = PermissionType.WRITE;
          } else if (line.startsWith("TableScan")) {
            AtomicBoolean hasPartitionKey = new AtomicBoolean(true);
            StringBuilder noPartitionColumnInfo = new StringBuilder();
            resources = parseTableScanPlan(reader, line, isInfoQuery,
                lastLine, hasPartitionKey, noPartitionColumnInfo);
            if (!hasPartitionKey.get()) {
              errorMessage.append(
                  "No partition column(" + noPartitionColumnInfo + ") in where clause.");
              return AclResult.NEED_PARTITION_COLUMN;
            }
            requiredPermission = PermissionType.READ;
          } else if (line.startsWith("DROP")) {
            resources = parseDropPlan(line);
            requiredPermission = PermissionType.WRITE;
          } else {
            isAclTarget = false;
          }
          if (isAclTarget) {
            if (resources == null) {
              errorMessage.append("Can't parse execution plan");
              LOG.error("Can't parse execution plan: " + queryPlan);
              return AclResult.DENY;
            }
            for (AccessControlResource eachResource : resources) {
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

  public List<AccessControlResource> parseTableCommitPlan(String currentLine) {
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
    String tableName = schemaTableTokens[1];

    List<AccessControlResource> resources = new ArrayList<AccessControlResource>();
    AccessControlResource resource = new AccessControlResource(catalog, schema, null, null);
    resources.add(resource);

    LOG.debug("Add Create AccessControlResource: " + resource);
    return resources;
  }

  public List<AccessControlResource> parseTableScanPlan(
      BufferedReader reader,
      String currentLine,
      boolean isInfoQuery,
      StringBuilder lastReadLine,
      AtomicBoolean hasPartitionKey,
      StringBuilder noPartitionColumnInfo) throws IOException {

    int startPos = currentLine.indexOf("[");
    if (startPos <= 0) {
      return null;
    }

    String[] tokens = currentLine.substring(startPos + 1, currentLine.indexOf(",")).split(":");
    String catalog = tokens[1];
    String schema = null;
    String table = null;

    if (catalog.startsWith("Kafka")) {
      catalog = "kafka";
    }

    if (isInfoQuery) {
      String key = "\"table_schema\" = '";
      int pos = currentLine.indexOf(key);
      String str = currentLine.substring(pos + key.length()).trim();
      schema = str.substring(0, str.indexOf("'"));

      key = "\"table_name\" = '";
      pos = currentLine.indexOf(key);
      if (pos >= 0) {
        str = currentLine.substring(pos + key.length()).trim();
        table = str.substring(0, str.indexOf("'"));
      }
      List<AccessControlResource> resources = new ArrayList<AccessControlResource>();
      AccessControlResource resource = new AccessControlResource(catalog, schema, table, null);
      resources.add(resource);
      LOG.info("Add Scan AccessControlResource: " + resource);
      return resources;
    }

    if (hiveCatalogNames.contains(catalog)) {
      schema = tokens[2];
      table = tokens[3];
    } else if (catalog.equals("kafka")) {
      String key = "schemaName=";
      int pos = currentLine.indexOf(key);
      String str = currentLine.substring(pos + key.length()).trim();
      schema = str.substring(0, str.indexOf(","));

      key = "tableName=";
      pos = currentLine.indexOf(key);
      if (pos >= 0) {
        str = currentLine.substring(pos + key.length()).trim();
        table = str.substring(0, str.indexOf(","));
      }
      List<AccessControlResource> resources = new ArrayList<AccessControlResource>();
      AccessControlResource resource = new AccessControlResource(catalog, schema, table, null);
      resources.add(resource);
      LOG.info("Add Scan AccessControlResource: " + resource);
      return resources;
    } else {
      String[] schemaTableTokens = tokens[2].split("\\.");
      if (schemaTableTokens.length < 2) {
        throw new IOException("Wrong query plan token, catalog=[" + catalog + "]" +
            " token=[" + tokens[2] + "], totalLine=[" + currentLine + "]");
      }
      schema = schemaTableTokens[0];
      table = schemaTableTokens[1];
    }

    List<AccessControlResource> resources = new ArrayList<AccessControlResource>();
    AccessControlResource resource = new AccessControlResource(catalog, schema, table, null);
    resources.add(resource);
    LOG.debug("Add Scan AccessControlResource: " + resource);

    String line = null;
    while ((line = reader.readLine()) != null) {
      line = line.trim();
      if (line.startsWith("-") || line.startsWith("DROP")) {
        break;
      }

      int pos = line.indexOf("ColumnHandle{");
      if (pos < 0) {
        continue;
      }

      tokens = line.substring(pos + "ColumnHandle{".length(), line.getBytes().length - 1)
          .split(",");

      for (String eachToken: tokens) {
        String[] subTokens = eachToken.trim().split("=");
        if (subTokens[0].trim().equals("name") || subTokens[0].trim().equals("columnName")) {
          resource = new AccessControlResource(catalog, schema, table, subTokens[1].trim());
          resources.add(resource);
          LOG.info("Add Scan AccessControlResource: " + resource);
        }
      }
    }

    String qualifiedName = catalog + "." + schema + "." + table;
    boolean needPartitonColumn = hiveCatalogNames.contains(catalog) &&
        partitionColumnNames.containsKey(qualifiedName);

    if (needPartitonColumn) {
      String partitionColumnName = partitionColumnNames.get(qualifiedName);
      boolean hasPartitionColumn = currentLine.indexOf("\"" + partitionColumnName) > 0;
      hasPartitionKey.set(hasPartitionColumn);
      noPartitionColumnInfo.append(qualifiedName + "." + partitionColumnName);
    } else {
      hasPartitionKey.set(true);
    }
    if (line != null) {
      lastReadLine.append(line);
    }
    return resources;
  }

  private List<AccessControlResource> parseDropPlan(String currentLine) {
    int pos = currentLine.indexOf("DROP TABLE ");
    if (pos < 0) {
      return null;
    }

    String[] tokens = currentLine.substring(pos + "DROP TABLE ".length()).trim().split("\\.");
    List<AccessControlResource> resources = new ArrayList<AccessControlResource>();
    AccessControlResource resource =
        new AccessControlResource(tokens[0], tokens[1], tokens[2], null);
    resources.add(resource);
    LOG.info("Add Drop AccessControlResource: " + resource);

    return resources;
  }

  public boolean canAccess(String principal,
                           AccessControlResource resource,
                           PermissionType permissionType) {
    if (acls.isEmpty()) {
      LOG.warn("No ACL properties. Please check your presto-acl.properties file");
      return true;
    }

    if (resource.getCatalog() == null) {
      return false;
    }

    Map<AccessControlResource, Set<PermissionType>> userAcls = acls.get(principal);
    if (userAcls == null) {
      return false;
    }

    // Check Catalog and Schema
    AccessControlResource targetAcl =
        new AccessControlResource(resource.getCatalog(), resource.getSchema(), null, null);

    Set<PermissionType> permissions = userAcls.get(targetAcl);
    if (permissions == null || !permissions.contains(permissionType)) {
      return false;
    }

    // Check Table
    if (resource.getTable() == null) {
      return true;
    }

    boolean allowAllTables = false;
    boolean denyAllTables = false;

    targetAcl.setTable("*");
    permissions = userAcls.get(targetAcl);
    if (permissions != null) {
      if (permissions.contains(PermissionType.ALLOW)) {
        allowAllTables = true;
      }
      if (permissions.contains(PermissionType.DENY)) {
        denyAllTables = true;
      }
    }

    targetAcl.setTable(resource.getTable());
    permissions = userAcls.get(targetAcl);
    if (permissions == null) {
      if (!allowAllTables) {
        return false;
      }
    } else if (permissions.contains(PermissionType.DENY)) {
      return false;
    }

    // Check Table
    if (resource.getColumn() == null) {
      return true;
    }
    boolean allowAllColumns = false;

    targetAcl.setColumn("*");
    permissions = userAcls.get(targetAcl);
    if (permissions != null && permissions.contains(PermissionType.ALLOW)) {
      allowAllColumns = true;
    }
    targetAcl.setColumn(resource.getColumn());
    permissions = userAcls.get(targetAcl);
    if (permissions == null) {
      if (allowAllColumns) {
        return true;
      }
      // If all tables are allowed, all columns are allowed except the specified case.
      if (allowAllTables) {
        HashSet<AccessControlResource> allowedColumnTables =
            tablesHasAllowedColumns.get(principal);

        AccessControlResource tableAcl =
            new AccessControlResource(resource.getCatalog(),
                resource.getSchema(), resource.getTable(), null);

        if (allowedColumnTables != null &&
            allowedColumnTables.contains(tableAcl)) {
          // not included in the specified allowed column.
          return false;
        }
        return true;
      } else {
        return false;
      }
    } else {
      if (permissions.contains(PermissionType.DENY)) {
        return false;
      }
    }

    return true;
  }

  public Map<AccessControlResource, Set<PermissionType>> getPermissions(String userOrRole) {
    return acls.get(userOrRole);
  }
}
