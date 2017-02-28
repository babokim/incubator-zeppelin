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
import java.util.concurrent.atomic.AtomicInteger;

import com.facebook.presto.client.*;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.*;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.*;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.presto.AccessControlManager.AclResult;
import org.apache.zeppelin.resource.ResourcePool;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.json.JsonCodec.jsonCodec;

/**
 * Presto interpreter for Zeppelin.
 */
public class PrestoInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(PrestoInterpreter.class);

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
            .add(PRESTO_ACL_PROPERTY, "",
                "Presto ACL property file path(default is conf/presto-acl.properties)").build());
  }

  private int maxRowsinNotebook = 1000;
  private int maxLimitRow = 100000;
  private String resultDataDir;
  private long expireResult;

  private JsonCodec<QueryResults> queryResultsCodec;
  private HttpClient httpClient;
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
    StatementClient planStatement;
    StatementClient sqlStatement;
    QueryResults sqlQueryResult;
    QueryResults planQueryResult;

    AtomicBoolean reportProgress = new AtomicBoolean(false);
    AtomicBoolean queryCanceled = new AtomicBoolean(false);
    long timestamp;

    public ParagraphTask() {
      this.timestamp = System.currentTimeMillis();
    }

    public void setQueryResult(boolean planQuery, QueryResults queryResults) {
      if (planQuery) {
        planQueryResult = queryResults;
      } else {
        sqlQueryResult = queryResults;
      }
    }

    public synchronized void close() {
      reportProgress.set(false);
      queryCanceled.set(true);
      if (planStatement != null) {
        try {
          planStatement.close();
        } catch (Exception e) {
        }
      }
      planStatement = null;

      if (sqlStatement != null) {
        try {
          sqlStatement.close();
        } catch (Exception e) {
        }
      }
      sqlStatement = null;
    }

    public String getQueryResultId() {
      if (sqlQueryResult != null) {
        return sqlQueryResult.getId();
      } else if (planQueryResult != null) {
        return planQueryResult.getId();
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

        List<String> expiredParagraphIds = new ArrayList<String>();
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
      queryResultsCodec = jsonCodec(QueryResults.class);
      HttpClientConfig httpClientConfig = new HttpClientConfig();
      httpClientConfig.setConnectTimeout(new Duration(10, TimeUnit.SECONDS));
      httpClient = new JettyHttpClient(httpClientConfig);

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
            "presto-zeppelin-" + userId,
            getProperty(PRESTOSERVER_CATALOG),
            getProperty(PRESTOSERVER_SCHEMA),
            TimeZone.getDefault().getID(),
            Locale.getDefault(),
            new HashMap<String, String>(),
            "",
            false,
            new Duration(10, TimeUnit.SECONDS));

        prestoSessions.put(userId, prestoSession);
      }
      return prestoSession;
    }
  }

  @Override
  public void close() {
    try {
      if (httpClient != null) {
        httpClient.close();
      }

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
    ParagraphTask task = getParagraphTask(context);
    task.reportProgress.set(false);
    task.queryCanceled.set(false);

    try {
      AuthenticationInfo authInfo = context.getAuthenticationInfo();
      if (authInfo == null || sql.trim().toLowerCase().startsWith("explain")) {
        return executeSql(sql, context, true);
      } else {
        String planSql = "explain " + sql;
        InterpreterResult planResult = executeSql(planSql, context, true);
        if (planResult.code() == Code.ERROR) {
          return planResult;
        }
//        try {
//          AccessControlManager aclInstance = AccessControlManager.getInstance(property);
//          String plan = planResult.message().get(0).getData();
//          StringBuilder aclMessage = null;
//          boolean canAccess = false;
//          for (String principal : authInfo.getUserAndRoles()) {
//            aclMessage = new StringBuilder();
//            AclResult aclResult = aclInstance.checkAcl(sql, plan, principal, aclMessage);
//            if (aclResult == AclResult.OK) {
//              canAccess = true;
//              break;
//            } else if (aclResult == AclResult.NEED_PARTITION_COLUMN) {
//              canAccess = false;
//              break;
//            }
//          }
//          if (!canAccess) {
//            return new InterpreterResult(Code.ERROR,
//                "[" + StringUtils.join(authInfo.getUserAndRoles(), ",") + "]" +
//                    aclMessage.toString());
//          }
//        } catch (Exception e) {
//          e.printStackTrace();
//          return new InterpreterResult(Code.ERROR,
//              "Error while checking authority: " + e.getMessage());
//        }

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

  private static List<Column> getColumns(StatementClient client)
      throws Exception
  {
    while (client.isValid()) {
      QueryResults results = client.current();
      List<Column> columns = results.getColumns();
      if (columns != null) {
        return columns;
      }
      client.advance();
    }

    QueryResults results = client.finalResults();
    if (!client.isFailed()) {
      throw new Exception("Query has no columns " + results.getId());
    }
    throw new Exception(results.toString());
  }


  private InterpreterResult executeSql(String sql,
                                       InterpreterContext context,
                                       boolean planQuery) {
    ResultFileMeta resultFileMeta = null;
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
      StatementClient statementClient = new StatementClient(httpClient, queryResultsCodec,
          getClientSession(context.getAuthenticationInfo().getUser()), sql);

      if (statementClient.isFailed()) {
        throw new Exception(statementClient.finalResults().toString());
      }

      if (planQuery) {
        task.planStatement = statementClient;
      } else {
        task.sqlStatement = statementClient;
      }
      StringBuilder msg = new StringBuilder();

      boolean alreadyPutColumnName = false;
      boolean isSelectSql = sql.trim().toLowerCase().startsWith("select");
      boolean isExplainSql = sql.trim().toLowerCase().startsWith("explain");
      AtomicInteger receivedRows = new AtomicInteger(0);

      String resultFilePath =
          resultDataDir + "/" + context.getNoteId() + "_" + context.getParagraphId();
      File resultFile = new File(resultFilePath);
      if (resultFile.exists()) {
        resultFile.delete();
      }

      while (statementClient.isValid()) {
        if (Thread.currentThread().isInterrupted()) {
          statementClient.close();
          return new InterpreterResult(Code.ERROR, "Query canceled.");
        }
        QueryResults queryResults = statementClient.current();
        task.setQueryResult(planQuery, queryResults);

        if (!task.reportProgress.get() && !planQuery) {
          task.reportProgress.set(true);
        }
        Iterable<List<Object>> data  = queryResults.getData();
        statementClient.advance();
        if (data == null) {
          continue;
        }
        if (!alreadyPutColumnName) {
          List<Column> columns = queryResults.getColumns();
          String prefix = "";
          for (Column eachColumn: columns) {
            msg.append(prefix).append(eachColumn.getName());
            if (prefix.isEmpty()) {
              prefix = "\t";
            }
          }
          msg.append("\n");
          alreadyPutColumnName = true;
        }

        resultFileMeta = processData(context, data, receivedRows,
            isSelectSql, isExplainSql, msg, resultFileMeta);
      }  //end of while

      QueryResults queryResults = statementClient.finalResults();
      if (queryResults.getError() != null) {
        return new InterpreterResult(Code.ERROR, queryResults.getError().getMessage());
      }
      task.setQueryResult(planQuery, queryResults);

      if (resultFileMeta != null) {
        resultFileMeta.outStream.close();
      }

      InterpreterResult result = new InterpreterResult(Code.SUCCESS);

      String resultMessage = msg.toString();
      result.add(isExplainSql ? resultMessage : "%table " + resultMessage);
      return result;
    } catch (Exception ex) {
      ex.printStackTrace();
      logger.error("Can not run " + sql, ex);
      return new InterpreterResult(Code.ERROR, ex.getMessage());
    } finally {
      if (resultFileMeta != null && resultFileMeta.outStream != null) {
        try {
          resultFileMeta.outStream.close();
        } catch (IOException e) {
        }
      }
    }
  }

  private String resultToCsv(String resultMessage) {
    StringBuilder sb = new StringBuilder();
    String[] lines = resultMessage.split("\n");

    for (String eachLine: lines) {
      String[] tokens = eachLine.split("\t");
      String prefix = "";
      for (String eachToken: tokens) {
        sb.append(prefix).append("\"").append(eachToken.replace("\"", "'")).append("\"");
        prefix = ",";
      }
      sb.append("\n");
    }

    return sb.toString();
  }

  private ResultFileMeta processData(
      InterpreterContext context,
      Iterable<List<Object>> data,
      AtomicInteger receivedRows,
      boolean isSelectSql,
      boolean isExplainSql,
      StringBuilder msg,
      ResultFileMeta resultFileMeta) throws IOException {
    for (List<Object> row : data) {
      receivedRows.incrementAndGet();
      if (receivedRows.get() > maxRowsinNotebook && isSelectSql && resultFileMeta == null) {
        resultFileMeta = new ResultFileMeta();
        resultFileMeta.filePath =
            resultDataDir + "/" + context.getNoteId() + "_" + context.getParagraphId();

        resultFileMeta.outStream = new FileOutputStream(resultFileMeta.filePath);
        resultFileMeta.outStream.write(0xEF);
        resultFileMeta.outStream.write(0xBB);
        resultFileMeta.outStream.write(0xBF);
        resultFileMeta.outStream.write(resultToCsv(msg.toString()).getBytes("UTF-8"));
      }
      String delimiter = "";
      String csvDelimiter = "";
      for (Object col : row) {
        String colStr =
            (col == null ? "null" :
                col.toString().replace('\n', ' ').replace('\r', ' ')
                    .replace('\t', ' ').replace('\"', '\''));
        if (receivedRows.get() > maxRowsinNotebook) {
          resultFileMeta.outStream.write((csvDelimiter + "\"" + colStr + "\"").getBytes("UTF-8"));
        } else {
          msg.append(delimiter).append(isExplainSql ? col.toString() : colStr);
        }

        if (delimiter.isEmpty()) {
          delimiter = "\t";
          csvDelimiter = ",";
        }
      }
      if (receivedRows.get() > maxRowsinNotebook) {
        resultFileMeta.outStream.write(("\n").getBytes());
      } else {
        msg.append("\n");
      }
    }
    return resultFileMeta;
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
      if (task.planStatement == null && task.sqlStatement == null) {
        return;
      }

      logger.info("Kill query '" + task.getQueryResultId() + "'");

      ResponseHandler handler = StringResponseHandler.createStringResponseHandler();
      Request request = prepareDelete().setUri(
          uriBuilderFrom(prestoServer).replacePath("/v1/query/" +
              task.getQueryResultId()).build()).build();
      try {
        httpClient.execute(request, handler);
        task.close();
      } catch (Exception e) {
        logger.error("Can not kill query " + task.getQueryResultId(), e);
      }
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
    if (!task.reportProgress.get() || task.sqlQueryResult == null) {
      return 0;
    }
    StatementStats stats = task.sqlQueryResult.getStats();
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
    property.setProperty(PRESTOSERVER_URL, "http://localhost:8090");
    property.setProperty(PRESTOSERVER_CATALOG, "hive");
    property.setProperty(PRESTOSERVER_SCHEMA, "ko");
    property.setProperty(PRESTOSERVER_USER, "hadoop");
    property.setProperty(PRESTOSERVER_PASSWORD, "1234");
    property.setProperty(PRESTO_MAX_RESULT_ROW, "100");
    property.setProperty(PRESTO_MAX_ROW, "1000");
    property.setProperty(PRESTO_ACL_PROPERTY,
        "/Users/hyoungjunkim/work/workspace/zeppelin/conf/presto-acl.properties");

    String sql = "select * from mysql.test.test \n" +
        "limit 5";

    PrestoInterpreter presto = new PrestoInterpreter(property);
    presto.open();

    HashSet<String> userAndRoles = new HashSet<String>();
    userAndRoles.add("emp");

    InterpreterContext context = new InterpreterContext(
        "noteId1",
        "paragraphId1",
        "replName",
        "paragraphTitle",
        "paragraphText",
        new AuthenticationInfo("user2", null, userAndRoles),
        null, //Map<String, Object> config,
        null, //GUI gui,
        null, //AngularObjectRegistry angularObjectRegistry,
        null, //ResourcePool resourcePool,
        null, //List<InterpreterContextRunner> runners,
        null  //InterpreterOutput out
    );

    InterpreterResult aclResult = presto.checkAclAndExecuteSql(sql, context);
    if (aclResult.code() == Code.ERROR) {
      System.out.println("ACL error: " + aclResult.message());
      return;
    }
    InterpreterResult result = presto.executeSql(sql, context, true);

    System.out.println("====Result====");
    System.out.println(result.message());
    System.out.println("==============");
    presto.close();
  }
}
