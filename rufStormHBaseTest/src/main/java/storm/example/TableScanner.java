package storm.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TableScanner {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", "5181");
        config.set("hbase.rootdir", "maprfs:///hbase");
        HTable table = new HTable(config, "/t1");

        Scan s = new Scan();
        s.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
        s.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"));
        ResultScanner scanner = table.getScanner(s);
        try {
          // Scanners return Result instances.
          // Now, for the actual iteration. One way is to use a while loop like so:
          for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            // print out the row we found and the columns we were looking for
            System.out.println("Found row: " + rr);
          }

          // The other approach is to use a foreach loop. Scanners are iterable!
          // for (Result rr : scanner) {
          //   System.out.println("Found row: " + rr);
          // }
        } finally {
          // Make sure you close your scanners when you are done!
          // Thats why we have it inside a try/finally clause
          scanner.close();
        }
    }
}
