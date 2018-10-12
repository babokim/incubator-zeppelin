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

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.facebook.presto.client.*;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.zeppelin.interpreter.*;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.presto.AccessControlManager.AclResult;
import org.apache.zeppelin.user.AuthenticationInfo;

import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.facebook.presto.client.OkHttpUtil.*;
import static com.facebook.presto.client.StatementClientFactory.newStatementClient;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Presto interpreter for Zeppelin.
 */
public class PrestoInterpreter extends Interpreter {
  private Logger logger = LoggerFactory.getLogger(PrestoInterpreter.class);

  static final String PRESTOSERVER_URL = "presto.url";
  static final String PRESTOSERVER_CATALOG = "presto.catalog";
  static final String PRESTOSERVER_SCHEMA = "presto.schema";
  static final String PRESTOSERVER_USER = "presto.user";
  static final String PRESTOSERVER_PASSWORD = "presto.password";
  static final String PRESTO_MAX_RESULT_ROW = "presto.notebook.rows.max";
  static final String PRESTO_MAX_ROW = "presto.rows.max";
  static final String PRESTO_RESULT_PATH = "presto.result.path";
  static final String PRESTO_RESULT_EXPIRE_DAY = "presto.result.expire";
  static final String PRESTO_ACL_PROPERTY = "presto.acl.properties.file";
  static final String PRESTO_ACL_ENABLE = "presto.acl.enable";

  static {
    Interpreter.register(
        "presto",
        "presto",
        PrestoInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
            .add(PRESTOSERVER_URL, "http://localhost:9090", "The URL for Presto")
            .add(PRESTOSERVER_CATALOG, "hive", "Default catalog")
            .add(PRESTOSERVER_SCHEMA, "default", "Default schema")
            .add(PRESTOSERVER_USER, "Presto", "The Presto user")
            .add(PRESTO_MAX_RESULT_ROW, "1000", "Maximum result rows on the notebook.")
            .add(PRESTO_MAX_ROW, "100000", "Maximum result rows in a query.")
            .add(PRESTO_RESULT_PATH, "/tmp/zeppelin-user", "Temporary directory for result data.")
            .add(PRESTO_RESULT_EXPIRE_DAY, "2", "Result data will be expire after this day.")
            .add(PRESTOSERVER_USER, "Presto", "The Presto user")
            .add(PRESTOSERVER_PASSWORD, "", "The password for the Presto user")
            .add(PRESTO_ACL_ENABLE, "true", "Enable ACL")
            .add(PRESTO_ACL_PROPERTY, "",
                "Presto ACL property file path(default is conf/presto-acl.properties)").build());
  }

  int maxRowsinNotebook = 1000;
  int maxLimitRow = 100000;
  String resultDataDir;
  private long expireResult;

  private JsonCodec<QueryResults> queryResultsCodec;
  private OkHttpClient httpClient;
  private Map<String, ClientSession> prestoSessions = new HashMap<String, ClientSession>();
  private Exception exceptionOnConnect;
  private URI prestoServer;
  private CleanResultFileThread cleanThread;

  private Map<String, ParagraphTask> paragraphTasks =
      new HashMap<String, ParagraphTask>();

  public PrestoInterpreter(Properties property) {
    super(property);
  }

  class ParagraphTask {
    PrestoQuery planQuery;
    PrestoQuery sqlQuery;

    AtomicBoolean reportProgress = new AtomicBoolean(false);
    AtomicBoolean queryCanceled = new AtomicBoolean(false);
    long timestamp;

    public ParagraphTask() {
      this.timestamp = System.currentTimeMillis();
    }

    public synchronized void close() {
      reportProgress.set(false);
      queryCanceled.set(true);
      if (planQuery != null) {
        try {
          planQuery.close();
        } catch (Exception e) {
        }
      }
      planQuery = null;

      if (sqlQuery != null) {
        try {
          sqlQuery.close();
        } catch (Exception e) {
        }
      }
      sqlQuery = null;
    }

    public URI getQueryResultId() {
      if (sqlQuery != null) {
        return sqlQuery.client.currentStatusInfo().getPartialCancelUri();
      } else if (planQuery != null) {
        return planQuery.client.currentStatusInfo().getPartialCancelUri();
      } else {
        return null;
      }
    }
  }

  class CleanResultFileThread extends Thread {
    @Override
    public void run() {
      long fourHour = 4 * 60 * 60 * 1000;
      logger.info("Presto result file cleaner started.");
      while (true) {
        try {
          Thread.sleep(10 * 60 * 1000);
        } catch (InterruptedException e) {
          break;
        }
        long currentTime = System.currentTimeMillis();

        List<String> expiredParagraphIds = new ArrayList<>();
        synchronized (paragraphTasks) {
          for (Map.Entry<String, ParagraphTask> entry : paragraphTasks.entrySet()) {
            ParagraphTask task = entry.getValue();
            if (currentTime - task.timestamp > fourHour) {
              task.close();
              expiredParagraphIds.add(entry.getKey());
            }
          }

          for (String paragraphId : expiredParagraphIds) {
            paragraphTasks.remove(paragraphId);
          }
        }

        try {
          File file = new File(resultDataDir);
          if (!file.exists()) {
            continue;
          }
          if (!file.isDirectory()) {
            logger.error(file + " is not directory.");
            continue;
          }

          File[] files = file.listFiles();
          if (files != null) {
            for (File eachFile: files) {
              if (eachFile.isDirectory()) {
                continue;
              }

              if (currentTime - eachFile.lastModified() >= expireResult) {
                logger.info("Delete " + eachFile + " because of expired.");
                eachFile.delete();
              }
            }
          }
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        }
      }
      logger.info("Presto result file cleaner stopped.");
    }
  }

  @Override
  public void open() {
    logger.info("Presto interpreter open called!");

    try {
      String maxRowsProperty = getProperty(PRESTO_MAX_RESULT_ROW);
      if (maxRowsProperty != null) {
        try {
          maxRowsinNotebook = Integer.parseInt(maxRowsProperty);
        } catch (Exception e) {
          logger.error("presto.notebook.rows.max property error: " + e.getMessage());
          maxRowsinNotebook = 1000;
        }
      }

      String maxLimitRowsProperty = getProperty(PRESTO_MAX_ROW);
      if (maxLimitRowsProperty != null) {
        try {
          maxLimitRow = Integer.parseInt(maxLimitRowsProperty);
        } catch (Exception e) {
          logger.error("presto.rows.max property error: " + e.getMessage());
          maxLimitRow = 100000;
        }
      }

      String expireResultProperty = getProperty(PRESTO_RESULT_EXPIRE_DAY);
      if (expireResultProperty != null) {
        try {
          expireResult = Integer.parseInt(expireResultProperty) * 60 * 60 * 24 * 1000;
        } catch (Exception e) {
          expireResult = 2 * 60 * 60 * 24 * 1000;
        }
      }

      resultDataDir = getProperty(PRESTO_RESULT_PATH);
      if (resultDataDir == null) {
        resultDataDir = "/tmp/zeppelin-" + System.getProperty("user.name");
      }

      File file = new File(resultDataDir);
      if (!file.exists()) {
        if (!file.mkdir()){
          logger.error("Can't make result directory: " + file);
        } else {
          logger.info("Created result directory: " + file);
        }
      }

      prestoServer =  new URI(getProperty(PRESTOSERVER_URL));

      OkHttpClient.Builder builder = new OkHttpClient.Builder();
      setupTimeouts(builder, 10, SECONDS);
      setupCookieJar(builder);
      this.httpClient = builder.build();

      cleanThread = new CleanResultFileThread();
      cleanThread.start();
      logger.info("Presto interpreter is opened!");
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      exceptionOnConnect = e;
    }
  }

  private ClientSession getClientSession(String userId) throws Exception {
    synchronized (prestoSessions) {
      ClientSession prestoSession = prestoSessions.get(userId);
      if (prestoSession == null) {
        prestoSession = new ClientSession(
            prestoServer,
            getProperty(PRESTOSERVER_USER),
            "zeppelin-presto-interpreter",
            Optional.of("presto-zeppelin-" + userId),
            new HashSet<String>(),
            "",
            getProperty(PRESTOSERVER_CATALOG),
            getProperty(PRESTOSERVER_SCHEMA),
            null,
            TimeZone.getDefault().getID(),
            Locale.getDefault(),
            new HashMap<String, String>(),
            new HashMap<String, String>(),
            new HashMap<String, String>(),
            null,
            new Duration(10, TimeUnit.SECONDS));

        prestoSessions.put(userId, prestoSession);
      }
      return prestoSession;
    }
  }

  @Override
  public void close() {
    try {
      cleanThread.interrupt();
    } finally {
      httpClient = null;
      exceptionOnConnect = null;
      cleanThread = null;
    }

    synchronized (paragraphTasks) {
      for (ParagraphTask task: paragraphTasks.values()) {
        task.close();
      }
      paragraphTasks.clear();
    }
  }

  private ParagraphTask getParagraphTask(InterpreterContext context) {
    synchronized (paragraphTasks) {
      ParagraphTask task = paragraphTasks.get(context.getParagraphId());
      if (task == null) {
        task = new ParagraphTask();
        paragraphTasks.put(context.getParagraphId(), task);
      }

      return task;
    }
  }

  private void removeParagraph(InterpreterContext context) {
    synchronized (paragraphTasks) {
      paragraphTasks.remove(context.getParagraphId());
    }
  }

  private InterpreterResult checkAclAndExecuteSql(String sql,
                                                  InterpreterContext context) {
    if (sql.equals("reload")) {
      try {
        AccessControlManager aclInstance = AccessControlManager.getInstance(property);
        aclInstance.loadConfig();
        return new InterpreterResult(Code.SUCCESS, "Reloaded.");
      } catch (Exception e) {
        e.printStackTrace();
        logger.error(e.getMessage(), e);
        return new InterpreterResult(Code.ERROR,
            "Error while reload config: " + e.getMessage());
      }
    }
    ParagraphTask task = getParagraphTask(context);
    task.reportProgress.set(false);
    task.queryCanceled.set(false);

    try {
      AuthenticationInfo authInfo = context.getAuthenticationInfo();
      if (authInfo.getUserAndRoles() == null || authInfo.getUserAndRoles().isEmpty()) {
        return new InterpreterResult(Code.ERROR, "Not login user.");
      }
      if (authInfo == null || sql.trim().toLowerCase().startsWith("explain")) {
        return executeSql(sql, context, true);
      } else {
        String planSql = "explain " + sql;
        InterpreterResult planResult = executeSql(planSql, context, true);
        if (planResult.code() == Code.ERROR) {
          return planResult;
        }
        try {
          AccessControlManager aclInstance = AccessControlManager.getInstance(property);
          String plan = planResult.message().get(0).getData();
          StringBuilder aclMessage = null;
          boolean canAccess = false;
          for (String principal : authInfo.getUserAndRoles()) {
            aclMessage = new StringBuilder();
            AclResult aclResult = aclInstance.checkAcl(sql, plan, principal, aclMessage);
            if (aclResult == AclResult.OK) {
              canAccess = true;
              break;
            } else if (aclResult == AclResult.NEED_PARTITION_COLUMN) {
              canAccess = false;
              break;
            }
          }
          if (!canAccess) {
            return new InterpreterResult(Code.ERROR,
                "[" + StringUtils.join(authInfo.getUserAndRoles(), ",") + "]" +
                    aclMessage.toString());
          }
        } catch (Exception e) {
//          e.printStackTrace();
          logger.error(e.getMessage(), e);
          return new InterpreterResult(Code.ERROR,
              "Error while checking authority: " + e.getMessage());
        }

        if (!task.queryCanceled.get()) {
          return executeSql(sql, context, false);
        } else {
          return new InterpreterResult(Code.ERROR, "Query canceled.");
        }
      }
    } finally {
      task.close();
    }
  }

  private InterpreterResult executeSql(String sql,
                                       InterpreterContext context,
                                       boolean planQuery) {
    ParagraphTask task = getParagraphTask(context);
    try {
      if (sql == null || sql.trim().isEmpty()) {
        return new InterpreterResult(Code.ERROR, "No query");
      }
      InterpreterResult limitCheckResult = assertLimitClause(sql);
      if (limitCheckResult.code() != Code.SUCCESS) {
        return limitCheckResult;
      }

      if (exceptionOnConnect != null) {
        return new InterpreterResult(Code.ERROR, exceptionOnConnect.getMessage());
      }

      OkHttpClient.Builder builder = httpClient.newBuilder();
      OkHttpClient client = builder.build();

      StatementClient statementClient = newStatementClient(client, getClientSession(context.getAuthenticationInfo().getUser()), sql);
      PrestoQuery query = new PrestoQuery(this, context, statementClient, sql);
      query.processQuery();
      String errorMessage = query.getErrorMessage();
      if (!errorMessage.isEmpty()) {
        InterpreterResult result = new InterpreterResult(Code.ERROR);
        result.add(errorMessage);
        return result;
      }

      if (planQuery) {
        task.planQuery = query;
      } else {
        task.sqlQuery = query;
      }

      InterpreterResult result = new InterpreterResult(Code.SUCCESS);
      result.add(query.getQueryResult());
      return result;
    } catch (Exception ex) {
      logger.error("Can not run " + sql, ex);
//      if (ex.getMessage() != null && ex.getMessage().indexOf("QueryResults") >= 0 ) {
//        String errorMessage = ex.getMessage();
//        int index = errorMessage.indexOf("error=QueryError{message=");
//        if (index > 0) {
//          String realMessage = errorMessage.substring(index + "error=QueryError{message=".length(), errorMessage.indexOf(", sqlState="));
//          return new InterpreterResult(Code.ERROR, realMessage);
//        }
//      }
      return new InterpreterResult(Code.ERROR, ex.getMessage());
    }
  }

  private InterpreterResult assertLimitClause(String sql) {
    String parsedSql = sql.trim().toLowerCase();
    if (parsedSql.startsWith("show") || parsedSql.startsWith("desc") ||
        parsedSql.startsWith("create") || parsedSql.startsWith("insert") ||
        parsedSql.startsWith("explain")) {
      return new InterpreterResult(Code.SUCCESS, "");
    }
    if (parsedSql.startsWith("select")) {
      parsedSql = parsedSql.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
      String[] tokens = parsedSql.replace("   ", " ").replace("  ", " ").split(" ");
      if (tokens.length < 2) {
        return new InterpreterResult(Code.ERROR, "No limit clause.");
      }

      if (tokens[tokens.length - 2].trim().equals("limit")) {
        int limit = Integer.parseInt(tokens[tokens.length - 1].trim());
        if (limit > maxLimitRow) {
          return new InterpreterResult(Code.ERROR, "Limit clause exceeds " + maxLimitRow);
        } else {
          return new InterpreterResult(Code.SUCCESS, "");
        }
      } else {
        return new InterpreterResult(Code.ERROR, "No limit clause.");
      }
    }
    return new InterpreterResult(Code.SUCCESS, "");
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext context) {
    AuthenticationInfo authInfo = context.getAuthenticationInfo();
    String user = authInfo == null ? "anonymous" : authInfo.getUser();
    logger.info("Run SQL command user['" + user + "'], [" + cmd + "]");
    return checkAclAndExecuteSql(cmd, context);
  }

  @Override
  public void cancel(InterpreterContext context) {
    ParagraphTask task = getParagraphTask(context);
    try {
      if (task.planQuery == null && task.sqlQuery == null) {
        return;
      }

      logger.info("Kill query '" + task.getQueryResultId() + "'");

      OkHttpClient.Builder builder = httpClient.newBuilder();
      OkHttpClient client = builder.build();

      try {
        Request request = new Request.Builder()
            .url(task.getQueryResultId().toURL())
            .build();

        client.newCall(request);
      } catch (Exception e) {
        logger.error("Can not kill query " + task.getQueryResultId(), e);
      }
//      ResponseHandler handler = StringResponseHandler.createStringResponseHandler();
//      Request request = prepareDelete().setUri(
//          uriBuilderFrom(prestoServer).replacePath("/v1/query/" +
//              task.getQueryResultId()).build()).build();
//      try {
//        httpClient.execute(request, handler);
//        task.close();
//      } catch (Exception e) {
//        logger.error("Can not kill query " + task.getQueryResultId(), e);
//      }
    } finally {
      removeParagraph(context);
    }
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    ParagraphTask task = getParagraphTask(context);
    if (!task.reportProgress.get() || task.sqlQuery == null) {
      return 0;
    }
    StatementStats stats = task.sqlQuery.client.getStats();
    if (stats.getTotalSplits() == 0) {
      return 0;
    } else {
      double p = (double) stats.getCompletedSplits() / (double) stats.getTotalSplits();
      return (int) (p * 100.0);
    }
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
        PrestoInterpreter.class.getName() + this.hashCode(), 5);
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor) {
    return null;
  }

  static class ResultFileMeta {
    String filePath;
    OutputStream outStream;
  }

  public static void main(String[] args) throws Exception {
    Properties property = new Properties();
    property.setProperty(PRESTOSERVER_URL, "http://122.112.235.109:18082");
    property.setProperty(PRESTOSERVER_CATALOG, "hive");
    property.setProperty(PRESTOSERVER_SCHEMA, "default");
    property.setProperty(PRESTOSERVER_USER, "hadoop");
    property.setProperty(PRESTOSERVER_PASSWORD, "1234");
    property.setProperty(PRESTO_MAX_RESULT_ROW, "100");
    property.setProperty(PRESTO_MAX_ROW, "100000");
    property.setProperty(PRESTO_ACL_ENABLE, "true");
    property.setProperty(PRESTO_ACL_PROPERTY,
        "/Users/babokim/work/workspace/zeppelin/conf/presto-acl.properties");

    String sql = "";

    System.out.println(sql);

    PrestoInterpreter presto = new PrestoInterpreter(property);
    presto.open();

    HashSet<String> userAndRoles = new HashSet<String>();
    userAndRoles.add("admin");

    InterpreterContext context = new InterpreterContext(
        "noteId1",
        "paragraphId1",
        "replName",
        "paragraphTitle",
        "paragraphText",
        new AuthenticationInfo("admin", null, userAndRoles),
        null, //Map<String, Object> config,
        null, //GUI gui,
        null, //AngularObjectRegistry angularObjectRegistry,
        null, //ResourcePool resourcePool,
        null, //List<InterpreterContextRunner> runners,
        null  //InterpreterOutput out
    );

    try {
      InterpreterResult result = presto.checkAclAndExecuteSql(sql, context);
      if (result.code() == Code.ERROR) {
        System.out.println("ACL error: " + result.message());
        return;
      }
      //    InterpreterResult result = presto.executeSql(sql, context, true);

      System.out.println("====Result====");
      System.out.println(result.message());
      System.out.println("==============");
    } finally {
      presto.close();
    }
  }
}
