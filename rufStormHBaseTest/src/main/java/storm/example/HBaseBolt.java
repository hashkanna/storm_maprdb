package storm.example;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ko3a4ok on 08.10.14.
 */
public class HBaseBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseBolt.class);

    public static final String CONFIG_KEY = "hbase.conf";
    private String tableName;
    private HTable table;
    private OutputCollector outputCollector;

    public HBaseBolt(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        final Configuration hbConfig = HBaseConfiguration.create();

        Map<String, Object> conf = (Map<String, Object>)stormConf.get(CONFIG_KEY);
        if(conf == null) {
            throw new IllegalArgumentException("HBase configuration not found using key '" + CONFIG_KEY + "'");
        }
        if(conf.get("hbase.rootdir") == null) {
            System.err.println("No 'hbase.rootdir' value found in configuration! Using HBase defaults.");
        }
        for(String key : conf.keySet()) {
            hbConfig.set(key, String.valueOf(conf.get(key)));
        }
        try {
            table = new HTable(hbConfig, tableName);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple input) {
        List mutations = new ArrayList(2);
        String word = input.getStringByField("word");
        int count = input.getIntegerByField("count");
        byte[] row = word.getBytes();
        byte[] cf = "cf".getBytes();
        byte[] cname = "name".getBytes();
        byte[] ccount = "count".getBytes();
        Put put = new Put(row);
        put.setDurability(Durability.SYNC_WAL);
        put.add(cf, cname, row);
        mutations.add(put);
        Increment increment = new Increment(row);
        increment.addColumn(cf, ccount, count);
        mutations.add(increment);
        batchMutate(mutations);
        /*Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", "5181");
        config.set("hbase.rootdir", "maprfs:///hbase");
        try {
          HTable table = new HTable(config, tableName);
          int n = 100;
          System.out.println(tableName);
          List<Put> puts = new ArrayList(n);
          for (int i = 0; i < n; i++) {
              Put put = new Put(("FromStandalone "+i).getBytes());
              put.add("cf".getBytes(), "iamcolumn".getBytes(), ("i am value #" + i).getBytes());
              puts.add(put);
              if ((i+1)%(n/10) == 0) System.out.println(100*(i+1)/n +"% is generate.");
          }
          System.out.println("Data is recording to the table.");
          //table.put(puts.get(0));
          table.batch(puts, new Object[n]);
          System.out.println("Recorded successfully.");
          table.close();
        } catch (Exception e) {
            System.out.println("Failed inserting data to table");
            e.printStackTrace();
        }*/ /*catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }

    public void batchMutate(List<Mutation> mutations) {
        Object[] result = new Object[mutations.size()];
        try {
            table.batch(mutations, result);
        } catch (InterruptedException e) {
            LOG.warn("Error performing a mutation to HBase.", e);
        } catch (IOException e) {
            LOG.warn("Error performing a mutation to HBase.", e);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
