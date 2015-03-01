package storm.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableValuesGenerator {
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length == 0) {
            System.out.println("Please, set table name.");
            return;
        }

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", "5181");
        config.set("hbase.rootdir", "maprfs:///hbase");
        HTable table = new HTable(config, args[0]);
        int n = 1000000;
        List<Put> puts = new ArrayList(n);
        for (int i = 0; i < n; i++) {
            Put put = new Put(("FromStandalone "+i).getBytes());
            put.add("cf".getBytes(), "iamcolumn".getBytes(), ("i am value #" + i).getBytes());
            puts.add(put);
            if ((i+1)%(n/10) == 0) System.out.println(100*(i+1)/n +"% is generate.");
        }
        System.out.println("Data is recording to the table.");
        table.batch(puts, new Object[n]);;
        System.out.println("Recorded successfully.");
        table.close();
    }
}