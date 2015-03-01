package storm.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class SpoutHbaseTopology {
    private static final String HBASE_SPOUT = "HBASE_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";

    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {

        if (args.length == 0) {
            System.out.println("Usage: SpoutHbaseTopology [table name] [topology name]");
            return;
        }
        final String tableName = args[0];

        TopologyBuilder builder = new TopologyBuilder();

        HBaseSpout spout = new HBaseSpout(tableName);

        BaseRichBolt bolt = new BaseRichBolt() {
            OutputCollector outputCollector;
            @Override
            public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
                this.outputCollector = outputCollector;
            }

            @Override
            public void execute(Tuple tuple) {
                String word = tuple.getStringByField(HBaseSpout.COLUMN_NAME);
                System.out.println("BOLT RECEIVE: " + word);
                outputCollector.emit(new Values(word));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
                outputFieldsDeclarer.declare(new Fields("output"));
            }
        };

        builder.setSpout(HBASE_SPOUT, spout, 1);
        builder.setBolt(COUNT_BOLT, bolt, 1).shuffleGrouping(HBASE_SPOUT);


        Config config = new Config();
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
            System.out.println("Usage: SpoutHbaseTopology [table name] [topology name]");
        }

    }
}
