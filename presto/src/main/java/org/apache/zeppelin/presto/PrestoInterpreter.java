/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.presto;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.facebook.presto.client.*;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.*;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.apache.zeppelin.interpreter.*;
import org.apache.commons.lang.StringUtils;
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
            .add(PRESTOSERVER_PASSWORD, "", "The password for the Presto user").build());
  }

  private int maxRowsinNotebook = 1000;
  private int maxLimitRow = 100000;
  private String resultDataDir;
  private long expireResult;

  private JsonCodec<QueryResults> queryResultsCodec;
  private HttpClient httpClient;
  private ClientSession prestoSession;
  private Exception exceptionOnConnect;
  private URI prestoServer;
  private CleanResultFileThread cleanThread;

  public PrestoInterpreter(Properties property) {
    super(property);
  }

  class CleanResultFileThread extends Thread {
    @Override
    public void run() {
      logger.info("Presto result file cleaner started.");
      while (true) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          break;
        }
        long currentTime = System.currentTimeMillis();
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
                logger.info("Delete " + eachFile + " beacuse of expired.");
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

      prestoSession = new ClientSession(
        prestoServer,
        getProperty(PRESTOSERVER_USER),
        "presto-zeppelin",
        getProperty(PRESTOSERVER_CATALOG),
        getProperty(PRESTOSERVER_SCHEMA),
        TimeZone.getDefault().getID(),
        Locale.getDefault(),
        ImmutableMap.<String, String>of(),
        null,
        false,
        new Duration(10, TimeUnit.SECONDS));

      cleanThread = new CleanResultFileThread();
      cleanThread.start();
      logger.info("Presto interpreter is opened!");
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      exceptionOnConnect = e;
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
  }

  StatementClient currentStatement;
  QueryResults currentQueryResult;

  private InterpreterResult executeSql(String sql, InterpreterContext context) {
    if (sql == null || sql.trim().isEmpty()) {
      return new InterpreterResult(Code.ERROR, "No query");
    }
    InterpreterResult limitCheckResult = assertLimitClause(sql);
    if (limitCheckResult.code() != Code.SUCCESS) {
      return limitCheckResult;
    }

    ResultFileMeta resultFileMeta = null;
    try {
      if (exceptionOnConnect != null) {
        return new InterpreterResult(Code.ERROR, exceptionOnConnect.getMessage());
      }
      currentStatement = new StatementClient(httpClient, queryResultsCodec, prestoSession, sql);
      StringBuilder msg = new StringBuilder();

      boolean alreadyPutColumnName = false;
      boolean isSelectSql = sql.trim().toLowerCase().startsWith("select");

      AtomicInteger receivedRows = new AtomicInteger(0);
      while (currentStatement != null && currentStatement.isValid() && currentStatement.advance()) {
        currentQueryResult = currentStatement.current();
        Iterable<List<Object>> data  = currentQueryResult.getData();
        if (data == null) {
          continue;
        }
        if (!alreadyPutColumnName) {
          List<Column> columns = currentQueryResult.getColumns();
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

        resultFileMeta = processData(context, data, receivedRows, isSelectSql, msg, resultFileMeta);
      }
      if (currentStatement != null) {
        currentQueryResult = currentStatement.finalResults();
        if (currentQueryResult.getError() != null) {
          return new InterpreterResult(Code.ERROR, currentQueryResult.getError().getMessage());
        }
      }
      if (resultFileMeta != null) {
        resultFileMeta.outStream.close();
      }

      InterpreterResult rett = new InterpreterResult(Code.SUCCESS,
          StringUtils.containsIgnoreCase(sql, "EXPLAIN ") ? msg.toString() :
            "%table " + msg.toString());
      return rett;
    } catch (Exception ex) {
      logger.error("Can not run " + sql, ex);
      return new InterpreterResult(Code.ERROR, ex.getMessage());
    } finally {
      if (currentStatement != null) {
        currentStatement.close();
      }
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
      StringBuilder msg,
      ResultFileMeta resultFileMeta) throws IOException {
    for (List<Object> row : data) {
      receivedRows.incrementAndGet();
      if (receivedRows.get() > maxRowsinNotebook && isSelectSql && resultFileMeta == null) {
        resultFileMeta = new ResultFileMeta();
        resultFileMeta.filePath =
            resultDataDir + "/" + context.getNoteId() + "_" + context.getParagraphId();

        resultFileMeta.outStream = new FileOutputStream(resultFileMeta.filePath);
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
          msg.append(delimiter).append(colStr);
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
    } else {
      return new InterpreterResult(Code.ERROR,
          "Only support show, desc, create, insert, select, explain.");
    }
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext context) {
    logger.info("Run SQL command '" + cmd + "'");
    return executeSql(cmd, context);
  }

  @Override
  public void cancel(InterpreterContext context) {
    if (currentStatement == null) {
      return;
    }

    logger.info("Kill query '" + currentQueryResult.getId() + "'");

    ResponseHandler handler = StringResponseHandler.createStringResponseHandler();
    Request request = prepareDelete().setUri(
        uriBuilderFrom(prestoServer).replacePath("/v1/query/" +
            currentQueryResult.getId()).build()).build();
    try {
      httpClient.execute(request, handler);
      currentStatement.close();
      currentStatement = null;
    } catch (Exception e) {
      logger.error("Can not kill query " + currentQueryResult.getId(), e);
    }
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    if (currentQueryResult == null) {
      return 0;
    }
    StatementStats stats = currentQueryResult.getStats();
    if (stats.getTotalSplits() == 0) {
      return 0;
    } else {
      double p = (double) stats.getCompletedSplits() / (double) stats.getTotalSplits();
      return (int) (p * 100.0);
    }
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetFIFOScheduler(
        PrestoInterpreter.class.getName() + this.hashCode());
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return null;
  }

  static class ResultFileMeta {
    String filePath;
    OutputStream outStream;
  }

  public static void main(String[] args) throws Exception {
    Properties property = new Properties();
    property.setProperty(PRESTOSERVER_URL, "http://localhost:9091");
    property.setProperty(PRESTOSERVER_CATALOG, "hive");
    property.setProperty(PRESTOSERVER_SCHEMA, "default");
    property.setProperty(PRESTOSERVER_USER, "hadoop");
    property.setProperty(PRESTOSERVER_PASSWORD, "1234");
    property.setProperty(PRESTO_MAX_RESULT_ROW, "10");
    property.setProperty(PRESTO_MAX_ROW, "100");

    String sql = "select * from jplog_map where dt = '20160303' limit \n 40";
    PrestoInterpreter presto = new PrestoInterpreter(property);
    presto.open();

    InterpreterResult result = presto.executeSql(sql, null);
    System.out.println(">>>>>>>Result:");
    System.out.println(result.message());

    // 채용 메일 A/B 테스트
    //=========================
    // 테마관 진행(차주 화요일 스테이징 배포 예정)
    // 테마관 API 진행, Push 알림 API 진행
  }
}
