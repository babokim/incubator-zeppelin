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

import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.presto.AccessControlManager.AclResult;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestAccessControlManager {
  class PlainFile {
    String plan = "";
    List<String> columnsInPlan = new ArrayList<>();

    public void loadPlanFile(File file) throws Exception {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      try {
        boolean planStarted = false;
        String line = null;
        while ( (line = reader.readLine()) != null ) {
          if (line.trim().isEmpty()) {
            continue;
          }
          if (line.startsWith("=====")) {
            planStarted = true;
            continue;
          }
          if (planStarted) {
            this.plan += line + "\n";
          } else {
            this.columnsInPlan.add(line.trim());
          }
        }
      } finally {
        reader.close();
      }
    }
  }

  @Test
  public void testPlanParser() throws Exception {
    File testDirectory = new File("src/test/resources");

    Properties property = new Properties();
    property.put(PrestoInterpreter.PRESTO_ACL_ENABLE, "true");
    property.put(PrestoInterpreter.PRESTO_ACL_PROPERTY, testDirectory.getAbsolutePath() + "/presto-acl.properties");

    AccessControlManager aclInstance = AccessControlManager.getInstance(property);


    File[] planFiles = (new File(testDirectory, "/plan")).listFiles();
    for (File eachPlanFile: planFiles) {
      if (!eachPlanFile.getName().equals("002.txt")) {
        continue;
      }
      System.out.println("Load plan file: " + eachPlanFile.getName());
      PlainFile plainFile = new PlainFile();
      plainFile.loadPlanFile(eachPlanFile);
      Set<String> columnsInPlan = new HashSet<String>();
      BufferedReader reader = new BufferedReader(new StringReader(plainFile.plan));
      String line = reader.readLine();
      while(line != null) {
        line = line.trim();
        if (line.startsWith("-")) {
          line = line.substring(1).trim();
        }
        StringBuilder lastLine = new StringBuilder();
        if (aclInstance.isScanOperator(line)) {
          columnsInPlan.addAll(aclInstance.parseTableScanPlan(reader, line, false, lastLine, null, null));
        }

        if (lastLine.length() > 0) {
          line = lastLine.toString();
        } else {
          line = reader.readLine();
        }
      }

      //assert
      System.out.println("1>>>>>" + StringUtils.join(plainFile.columnsInPlan, ","));
      System.out.println("2>>>>>" + StringUtils.join(columnsInPlan, ","));
      assertEquals("Different columns", plainFile.columnsInPlan.size(), columnsInPlan.size());
      for (String eachColumn: plainFile.columnsInPlan) {
        assertTrue("Column not exist in plan", columnsInPlan.remove(eachColumn));
      }
      assertTrue(columnsInPlan.size() == 0);
    }
  }

  @Test
  public void testParsePlan() throws Exception {
    String plan =
        " - Output[shop_code, area, retail_name, brand_code, average_daily_sales_per_square] => [shop_code:varchar, area:varchar, retail_name:varchar, brand_code_14:varchar, avg:double]\n" +
            "         brand_code := brand_code_14\n" +
            "         average_daily_sales_per_square := avg\n" +
            "     - RemoteMerge[avg DESC_NULLS_LAST] => [shop_code:varchar, area:varchar, retail_name:varchar, brand_code_14:varchar, avg:double]\n" +
            "         - LocalMerge[avg DESC_NULLS_LAST] => [shop_code:varchar, area:varchar, retail_name:varchar, brand_code_14:varchar, avg:double]\n" +
            "             - PartialSort[avg DESC_NULLS_LAST] => [shop_code:varchar, area:varchar, retail_name:varchar, brand_code_14:varchar, avg:double]\n" +
            "                 - RemoteExchange[REPARTITION] => shop_code:varchar, area:varchar, retail_name:varchar, brand_code_14:varchar, avg:double\n" +
            "                         Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                     - Project[] => [shop_code:varchar, area:varchar, retail_name:varchar, brand_code_14:varchar, avg:double]\n" +
            "                             Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                         - Aggregate(FINAL)[shop_code, area, retail_name, brand_code_14][$hashvalue] => [shop_code:varchar, area:varchar, retail_name:varchar, brand_code_14:varchar, $hashvalue:bigint, avg:double]\n" +
            "                                 Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                                 avg := \"avg\"(\"avg_70\")\n" +
            "                             - LocalExchange[HASH][$hashvalue] (\"shop_code\", \"area\", \"retail_name\", \"brand_code_14\") => shop_code:varchar, area:varchar, retail_name:varchar, brand_code_14:varchar, avg_70:row(double, bigint), $hashvalue:bigint\n" +
            "                                     Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                                 - Aggregate(PARTIAL)[shop_code, area, retail_name, brand_code_14][$hashvalue_80] => [shop_code:varchar, area:varchar, retail_name:varchar, brand_code_14:varchar, $hashvalue_80:bigint, avg_70:row(double, bigint)]\n" +
            "                                         Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                                         avg_70 := \"avg\"(\"expr_21\")\n" +
            "                                     - Project[] => [shop_code:varchar, area:varchar, expr_21:double, retail_name:varchar, brand_code_14:varchar, $hashvalue_80:bigint]\n" +
            "                                             Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                                             expr_21 := (CAST(\"sum\" AS double) / CAST(\"area\" AS double))\n" +
            "                                             $hashvalue_80 := \"combine_hash\"(\"combine_hash\"(\"combine_hash\"(\"combine_hash\"(bigint '0', COALESCE(\"$operator$hash_code\"(\"shop_code\"), 0)), COALESCE(\"$operator$hash_code\"(\"area\"), 0)), COALESCE(\"$operato\n" +
            "                                         - InnerJoin[(\"shop_code\" = \"code\")][$hashvalue_72, $hashvalue_77] => [shop_code:varchar, sum:decimal(38,0), brand_code_14:varchar, retail_name:varchar, area:varchar]\n" +
            "                                                 Distribution: PARTITIONED\n" +
            "                                                 Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                                             - RemoteExchange[REPARTITION][$hashvalue_72] => shop_code:varchar, sum:decimal(38,0), $hashvalue_72:bigint\n" +
            "                                                     Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                                                 - Project[] => [shop_code:varchar, sum:decimal(38,0), $hashvalue_76:bigint]\n" +
            "                                                         Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                                                         $hashvalue_76 := \"combine_hash\"(bigint '0', COALESCE(\"$operator$hash_code\"(\"shop_code\"), 0))\n" +
            "                                                     - Aggregate(FINAL)[shop_code, order_date][$hashvalue_73] => [shop_code:varchar, order_date:varchar, $hashvalue_73:bigint, sum:decimal(38,0)]\n" +
            "                                                             Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                                                             sum := \"sum\"(\"sum_71\")\n" +
            "                                                         - LocalExchange[HASH][$hashvalue_73] (\"shop_code\", \"order_date\") => shop_code:varchar, order_date:varchar, sum_71:varbinary, $hashvalue_73:bigint\n" +
            "                                                                 Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                                                             - RemoteExchange[REPARTITION][$hashvalue_74] => shop_code:varchar, order_date:varchar, sum_71:varbinary, $hashvalue_74:bigint\n" +
            "                                                                     Cost: {rows: ? (?), cpu: ?, memory: ?, network: ?}\n" +
            "                                                                 - Aggregate(PARTIAL)[shop_code, order_date][$hashvalue_75] => [shop_code:varchar, order_date:varchar, $hashvalue_75:bigint, sum_71:varbinary]\n" +
            "                                                                         Cost: {rows: ? (?), cpu: 0.00, memory: ?, network: 0.00}\n" +
            "                                                                         sum_71 := \"sum\"(\"expr\")\n" +
            "                                                                     - ScanProject[table = hive:csl:sale_mst, originalConstraint = ((\"order_date\" >= CAST('20180704' AS varchar)) AND (\"order_date\" < CAST('20180726' AS varchar)))] => [shop_code:varc\n" +
            "                                                                             Cost: {rows: 0 (0B), cpu: 0.00, memory: 0.00, network: 0.00}/{rows: 0 (0B), cpu: 0.00, memory: 0.00, network: 0.00}\n" +
            "                                                                             expr := CAST(\"estimate_sale_amt\" AS decimal)\n" +
            "                                                                             $hashvalue_75 := \"combine_hash\"(\"combine_hash\"(bigint '0', COALESCE(\"$operator$hash_code\"(\"shop_code\"), 0)), COALESCE(\"$operator$hash_code\"(\"order_date\"), 0))\n" +
            "                                                                             LAYOUT: csl.sale_mst\n" +
            "                                                                             order_date := HiveColumnHandle{name=order_date, hiveType=string, hiveColumnIndex=-1, columnType=PARTITION_KEY}\n" +
            "                                                                                 :: [[20180704], [20180705], [20180706], [20180707], [20180708], [20180709], [20180710], [20180711], [20180712], [20180713], [20180714], [20180715], [20180716], [20180\n" +
            "                                                                             estimate_sale_amt := HiveColumnHandle{name=estimate_sale_amt, hiveType=string, hiveColumnIndex=17, columnType=REGULAR}\n" +
            "                                                                             shop_code := HiveColumnHandle{name=shop_code, hiveType=string, hiveColumnIndex=2, columnType=REGULAR}\n" +
            "                                                                             HiveColumnHandle{name=brand_code, hiveType=string, hiveColumnIndex=-1, columnType=PARTITION_KEY}\n" +
            "                                                                                 :: [[BC], [CI], [CO], [EC], [EE], [EH], [EK], [EL], [GL], [KW], [MB], [MC], [MD], [NC], [PB], [PC], [PD], [PO], [PR], [Q3], [RA], [RC], [SA], [SF], [SM], [T0], [TN],\n" +
            "                                             - LocalExchange[HASH][$hashvalue_77] (\"code\") => brand_code_14:varchar, code:varchar, retail_name:varchar, area:varchar, $hashvalue_77:bigint\n" +
            "                                                     Cost: {rows: ? (?), cpu: ?, memory: 0.00, network: ?}\n" +
            "                                                 - RemoteExchange[REPARTITION][$hashvalue_78] => brand_code_14:varchar, code:varchar, retail_name:varchar, area:varchar, $hashvalue_78:bigint\n" +
            "                                                         Cost: {rows: ? (?), cpu: ?, memory: 0.00, network: ?}\n" +
            "                                                     - ScanFilterProject[table = hive:csl:shop_detail, originalConstraint = true, filterPredicate = (CAST(\"area\" AS double) > 1E1)] => [brand_code_14:varchar, code:varchar, retail_name:varchar, area:\n" +
            "                                                             Cost: {rows: ? (?), cpu: ?, memory: 0.00, network: 0.00}/{rows: ? (?), cpu: ?, memory: 0.00, network: 0.00}/{rows: ? (?), cpu: ?, memory: 0.00, network: 0.00}\n" +
            "                                                             $hashvalue_79 := \"combine_hash\"(bigint '0', COALESCE(\"$operator$hash_code\"(\"code\"), 0))\n" +
            "                                                             LAYOUT: csl.shop_detail\n" +
            "                                                             area := HiveColumnHandle{name=area, hiveType=string, hiveColumnIndex=17, columnType=REGULAR}\n" +
            "                                                             code := HiveColumnHandle{name=code, hiveType=string, hiveColumnIndex=1, columnType=REGULAR}\n" +
            "                                                             brand_code_14 := HiveColumnHandle{name=brand_code, hiveType=string, hiveColumnIndex=0, columnType=REGULAR}\n" +
            "                                                             retail_name := HiveColumnHandle{name=retail_name, hiveType=string, hiveColumnIndex=8, columnType=REGULAR}";

    Properties property = new Properties();
    property.put(PrestoInterpreter.PRESTO_ACL_ENABLE, "true");
    property.put(PrestoInterpreter.PRESTO_ACL_PROPERTY, "/Users/babokim/work/workspace/zeppelin/presto/src/test/resources/presto-acl.properties");

    AccessControlManager aclInstance = AccessControlManager.getInstance(property);
    StringBuilder aclMessage = new StringBuilder();
    AclResult aclResult = aclInstance.checkAcl("select * limit 100", plan, "admin", aclMessage);
    boolean canAccess = false;
    if (aclResult == AclResult.OK) {
      canAccess = true;
    } else if (aclResult == AclResult.NEED_PARTITION_COLUMN) {
      canAccess = false;
    }

    assertTrue("Admin can access", canAccess) ;

    aclMessage = new StringBuilder();
    aclResult = aclInstance.checkAcl("select * limit 100", plan, "customer", aclMessage);
    canAccess = false;
    if (aclResult == AclResult.OK) {
      canAccess = true;
    } else if (aclResult == AclResult.NEED_PARTITION_COLUMN) {
      canAccess = false;
    }

    assertFalse("Customer can't access", canAccess) ;
  }
}
