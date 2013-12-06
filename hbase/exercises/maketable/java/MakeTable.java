import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class MakeTable {

public static final byte[] TABLE_NAME = "USER".getBytes();
public static final byte[] COLFAM_NAME = "INFO".getBytes();
public static final byte[] COL_VALUE = "NAME".getBytes();

public static void main(String[] args) throws IOException,
InterruptedException {

Configuration conf = HBaseConfiguration.create();

HBaseAdmin admin = new HBaseAdmin(conf);

if(admin.tableExists(TABLE_NAME)) {
       admin.disableTable(TABLE_NAME);
       admin.deleteTable(TABLE_NAME);
}
HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
HColumnDescriptor coldef = new HColumnDescriptor(COLFAM_NAME);
desc.addFamily(coldef);
coldef.setMaxVersions(1);
admin.createTable(desc);

HTable userTable = new HTable(conf, TABLE_NAME);

Put row1 = new Put(Bytes.toBytes("42"));
row1.add(COLFAM_NAME, COL_VALUE, Bytes.toBytes("Diana"));

Put row2 = new Put(Bytes.toBytes("43"));
row2.add(COLFAM_NAME, COL_VALUE, Bytes.toBytes("Doug"));

Put row3 = new Put(Bytes.toBytes("44"));
row3.add(COLFAM_NAME, COL_VALUE, Bytes.toBytes("Steve"));

userTable.put(row1);
userTable.put(row2);
userTable.put(row3);

admin.flush(TABLE_NAME);

Scan userScan = new Scan();
ResultScanner scanner = userTable.getScanner(userScan);
for (Result result : scanner ) {
       System.out.println(Bytes.toString(result.getValue(COLFAM_NAME,
COL_VALUE)));
}

userTable.close();

 }
}

