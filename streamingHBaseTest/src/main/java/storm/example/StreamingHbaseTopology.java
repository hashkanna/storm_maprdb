/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.util.HashMap;
import java.util.Map;


public class StreamingHbaseTopology {
    private static final String HBASE_BOLT = "HBASE_BOLT";
    private static final String FILE_SPOUT = "FILE_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String ROLLING_COUNT_BOLT = "ROLLING_COUNT_BOLT";
    private static final String REPORT_BOLT = "REPORT_BOLT";


    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);

        Map<String, Object> hbConf = new HashMap<String, Object>();
        hbConf.put("hbase.zookeeper.property.clientPort", 5181);
        hbConf.put("hbase.rootdir", "maprfs:///hbase");
        config.put(HBaseBolt.CONFIG_KEY, hbConf);

        //WordSpout spout = new WordSpout();
        //WordCounter bolt = new WordCounter();
        if (args.length == 0) {
            System.out.println("Please, set table name.");
            return;
        }
        String tableName = args[0];
        HBaseBolt hbase = new HBaseBolt(tableName);
        // wordSpout ==> countBolt ==> HBaseBolt
        TopologyBuilder builder = new TopologyBuilder();

        //builder.setSpout(WORD_SPOUT, spout, 1);
        //builder.setBolt(COUNT_BOLT, bolt, 1).shuffleGrouping(WORD_SPOUT);
        //builder.setBolt(HBASE_BOLT, hbase, 1).fieldsGrouping(COUNT_BOLT, new Fields("word"));

        //builder.setSpout(HBASE_SPOUT, spout, 1);
        //builder.setSpout(FILE_SPOUT, new FileSpout("/root/aol_logs.txt", '\t', true));
        builder.setSpout(FILE_SPOUT, new FileSpout("/root/Omniture.0.tsv", '\t', false));
        //builder.setSpout(FILE_SPOUT, new FileSpout("/tmp/Omniture.0.tsv", '\t', false));
        //builder.setBolt(COUNT_BOLT, bolt, 1).shuffleGrouping(HBASE_SPOUT);
        //builder.setBolt(ROLLING_COUNT_BOLT, new RollingCountBolt(30, 10), 1).fieldsGrouping(FILE_SPOUT, new Fields("Query"));
        builder.setBolt(ROLLING_COUNT_BOLT, new RollingCountBolt(30, 10), 1).fieldsGrouping(FILE_SPOUT, new Fields("field_52"));
        //builder.setBolt(REPORT_BOLT, new ReportBolt(), 1).globalGrouping(ROLLING_COUNT_BOLT);
        builder.setBolt(HBASE_BOLT, hbase, 1).globalGrouping(ROLLING_COUNT_BOLT);


        if (args.length == 1) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Thread.sleep(30000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(0);
        } else if (args.length == 2) {
            StormSubmitter.submitTopology(args[1], config, builder.createTopology());
        } else{
            System.out.println("Usage: PersistentWordCount <hdfs url> [topology name]");
        }
    }
}
