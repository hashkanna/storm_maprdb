package storm.example;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class HBaseSpout extends BaseRichSpout {
    public static final String COLUMN_NAME = "word";

    private String tableName;
    private HTable table;
    private SpoutOutputCollector collector;
    private BlockingQueue<String> bq = new LinkedBlockingQueue();

    public HBaseSpout(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(COLUMN_NAME));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", "5181");
        config.set("hbase.rootdir", "maprfs:///hbase");
        try {
            table = new HTable(config, tableName);
            final ResultScanner res = table.getScanner("cf".getBytes());
            new Thread() {
                @Override
                public void run() {
                    for (Result result : res) {
                        String value = new String(result.value());
                        try {
                            bq.put(value);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void nextTuple() {
        while (!bq.isEmpty())
            collector.emit(new Values(bq.poll()), UUID.randomUUID());
        Thread.yield();
    }
}
