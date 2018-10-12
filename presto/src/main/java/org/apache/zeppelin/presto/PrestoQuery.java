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

import com.facebook.presto.cli.*;
import com.facebook.presto.client.*;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.presto.PrestoInterpreter.ResultFileMeta;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.cli.ConsolePrinter.REAL_TERMINAL;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoQuery implements Closeable {
  private Logger logger = LoggerFactory.getLogger(PrestoInterpreter.class);

  final StatementClient client;
  private StringBuilder queryResult = new StringBuilder();
  private AtomicInteger receivedRows = new AtomicInteger(0);
  private ResultFileMeta resultFileMeta;
  private final boolean isExplainSql;
  private final boolean isSelectSql;
  private final String sql;
  String resultFilePath;
  private ByteArrayOutputStream errorByteArray;
  private PrintStream errorChannel;
  private PrestoInterpreter prestoInterpreter;

  public PrestoQuery(PrestoInterpreter prestoInterpreter, InterpreterContext context, StatementClient client, String sql) {
    this.prestoInterpreter = prestoInterpreter;
    this.client = requireNonNull(client, "client is null");
    this.sql = sql;
    this.isExplainSql = sql.trim().toLowerCase().startsWith("explain");
    this.isSelectSql = sql.trim().toLowerCase().startsWith("select");
    this.resultFilePath =
        prestoInterpreter.resultDataDir + "/" + context.getNoteId() + "_" + context.getParagraphId();
    File resultFile = new File(resultFilePath);

    if (resultFile.exists()) {
      resultFile.delete();
    }
    errorByteArray = new ByteArrayOutputStream();
    errorChannel = new PrintStream(errorByteArray);
  }

  public String getQueryResult() {
    return isExplainSql ? queryResult.toString() : "%table " + queryResult.toString();
  }

  public String getErrorMessage() {
    return new String(errorByteArray.toByteArray());
  }

  public void processQuery() throws Exception {
      waitForData();

    // if running or finished
    if (client.isRunning() || (client.isFinished() && client.finalStatusInfo().getError() == null)) {
      QueryStatusInfo results = client.isRunning() ? client.currentStatusInfo() : client.finalStatusInfo();
      if (results.getUpdateType() != null) {
        renderUpdate(errorChannel, results);
      } else if (results.getColumns() == null) {
        errorChannel.printf("Query %s has no columns\n", results.getId());
      } else {
        processResults(results.getColumns());
      }
    }

    if (client.isFinished()) {
      if (client.finalStatusInfo().getError() != null) {
        renderFailure(errorChannel);
      }
    }
  }

  private void waitForData() {
    while (client.isRunning() && (client.currentData().getData() == null)) {
      if (Thread.currentThread().isInterrupted()) {
        this.close();
        errorChannel.printf("Query canceled.");
        return;
      }
      client.advance();
    }
  }

  private void renderUpdate(PrintStream out, QueryStatusInfo results)
  {
    String status = results.getUpdateType();
    if (results.getUpdateCount() != null) {
      long count = results.getUpdateCount();
      status += format(": %s row%s", count, (count != 1) ? "s" : "");
    }
    out.println(status);
    discardResults();
  }

  private void discardResults()
  {
    try (OutputHandler handler = new OutputHandler(new NullPrinter())) {
      handler.processRows(client);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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

  private void processResults(List<Column> columns) throws Exception {
    String prefix = "";
    for (Column eachColumn: columns) {
      queryResult.append(prefix).append(eachColumn.getName());
      if (prefix.isEmpty()) {
        prefix = "\t";
      }
    }
    queryResult.append("\n");

    try {
      while (client.isRunning()) {
        Iterable<List<Object>> data = client.currentData().getData();
        client.currentStatusInfo().getPartialCancelUri();
        if (data != null) {
          for (List<Object> row : data) {
            receivedRows.incrementAndGet();
            if (receivedRows.get() > prestoInterpreter.maxRowsinNotebook && isSelectSql && resultFileMeta == null) {
              resultFileMeta = new ResultFileMeta();
              resultFileMeta.filePath = resultFilePath;
              resultFileMeta.outStream = new FileOutputStream(resultFileMeta.filePath);
              resultFileMeta.outStream.write(0xEF);
              resultFileMeta.outStream.write(0xBB);
              resultFileMeta.outStream.write(0xBF);
              resultFileMeta.outStream.write(resultToCsv(queryResult.toString()).getBytes("UTF-8"));
            }

            String delimiter = "";
            String csvDelimiter = "";
            for (Object col : row) {
              String colStr =
                  (col == null ? "null" :
                      col.toString().replace('\n', ' ').replace('\r', ' ')
                          .replace('\t', ' ').replace('\"', '\''));
              if (receivedRows.get() > prestoInterpreter.maxRowsinNotebook) {
                resultFileMeta.outStream.write((csvDelimiter + "\"" + colStr + "\"").getBytes("UTF-8"));
              } else {
                queryResult.append(delimiter).append(isExplainSql ? col.toString() : colStr);
              }

              if (delimiter.isEmpty()) {
                delimiter = "\t";
                csvDelimiter = ",";
              }
            }
            if (receivedRows.get() > prestoInterpreter.maxRowsinNotebook) {
              resultFileMeta.outStream.write(("\n").getBytes());
            } else {
              queryResult.append("\n");
            }
          }
        }
        client.advance();
      }
    } finally {
      if (resultFileMeta != null && resultFileMeta.outStream != null) {
        resultFileMeta.outStream.close();
      }

    }
  }

  @Override
  public void close()
  {
    client.close();
  }

  public void renderFailure(PrintStream out)
  {
    QueryStatusInfo results = client.finalStatusInfo();
    QueryError error = results.getError();
    checkState(error != null);

    out.printf("Query %s failed: %s%n", results.getId(), error.getMessage());
//    if (error.getFailureInfo() != null) {
//      error.getFailureInfo().toException().printStackTrace(out);
//    }
    if (error.getErrorLocation() != null) {
      renderErrorLocation(client.getQuery(), error.getErrorLocation(), out);
    }
    out.println();
  }

  private static void renderErrorLocation(String query, ErrorLocation location, PrintStream out)
  {
    List<String> lines = ImmutableList.copyOf(Splitter.on('\n').split(query).iterator());

    String errorLine = lines.get(location.getLineNumber() - 1);
    String good = errorLine.substring(0, location.getColumnNumber() - 1);
    String bad = errorLine.substring(location.getColumnNumber() - 1);

    if ((location.getLineNumber() == lines.size()) && bad.trim().isEmpty()) {
      bad = " <EOF>";
    }

    if (REAL_TERMINAL) {
      Ansi ansi = Ansi.ansi();

      ansi.fg(Ansi.Color.CYAN);
      for (int i = 1; i < location.getLineNumber(); i++) {
        ansi.a(lines.get(i - 1)).newline();
      }
      ansi.a(good);

      ansi.fg(Ansi.Color.RED);
      ansi.a(bad).newline();
      for (int i = location.getLineNumber(); i < lines.size(); i++) {
        ansi.a(lines.get(i)).newline();
      }

      ansi.reset();
      out.print(ansi);
    }
    else {
      String prefix = format("LINE %s: ", location.getLineNumber());
      String padding = Strings.repeat(" ", prefix.length() + (location.getColumnNumber() - 1));
      out.println(prefix + errorLine);
      out.println(padding + "^");
    }
  }
}
