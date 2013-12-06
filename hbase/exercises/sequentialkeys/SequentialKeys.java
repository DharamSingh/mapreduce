import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class SequentialKeys {
private static final byte[] TABLE_EVENTS = "EVENTS".getBytes();
private static final byte[] COLFAM_VALUES = "values".getBytes();
private static final byte[] PAYLOAD_KEY = "payload".getBytes();
private static final byte[] PAYLOAD_VALUE = ".....".getBytes();
private static final String DATE_FORMAT = "yyyy-MM-dd-HH:mm:ss.SSS";
private static final String[] years = {
  "2006",
  "2007",
  "2008",
  "2009",
  "2010"
};
private static final Configuration conf = HBaseConfiguration.create();


public static void main(String[] args) {
 try {
  createTable();
  doInserts();
 } catch (Exception e) {
  e.printStackTrace();
 } finally {
  System.exit(0);
 }
}

private static void doInserts() throws Exception {
 HBaseAdmin hbase = new HBaseAdmin(conf);

 HTable eventsTable = new HTable(conf, TABLE_EVENTS);
 SimpleDateFormat dateFormater = new SimpleDateFormat(DATE_FORMAT);
 Random rand = new Random();
 for(String year : years) {
  String date = dateFormater.format(new Date());
  date = year + date.substring(4);
  int limit = rand.nextInt(15) + 1;
  // create `limit' store files
  for(int i = 0; i < limit; i++) {
   // do 2000 inserts per store file
   for (int j = 0; j < 2000; j++) {
    Put row = createNewEvent(date);
    eventsTable.put(row);
   }
   hbase.flush(TABLE_EVENTS); // create store files
  }
 }

 for (int i = 0; i < Integer.MAX_VALUE; i++) {
  String date = dateFormater.format(new Date());
  Put row = createNewEvent(date);
  eventsTable.put(row);
  if(i > 0 && i % 2000 == 0) {
   hbase.flush(TABLE_EVENTS);
  }
 }
 eventsTable.close();
}

private static Put createNewEvent(String date) {
 String uuid = java.util.UUID.randomUUID().toString();
 Put row = new Put(Bytes.toBytes(String.format("%s:%s", date, uuid)));
 row.add(COLFAM_VALUES, PAYLOAD_KEY, PAYLOAD_VALUE);
 return row;
}

private static void createTable() throws Exception {
 HBaseAdmin hbase = new HBaseAdmin(conf);
 if(hbase.tableExists(TABLE_EVENTS)) {
  hbase.disableTable(TABLE_EVENTS);
  hbase.deleteTable(TABLE_EVENTS);
 }
 HTableDescriptor desc = new HTableDescriptor(TABLE_EVENTS);
 HColumnDescriptor colDes = new HColumnDescriptor(COLFAM_VALUES);
 desc.addFamily(colDes);
 byte[][] splitKeys = new byte[years.length][];
 for (int i = 0; i < years.length; i++) {
  splitKeys[i] = years[i].getBytes();
 }
 hbase.createTable(desc, splitKeys);
}
}

