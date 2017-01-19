import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * create TABLE gcsdba.tutorial ( pk timeuuid, serial bigint, date timestamp, value text, columnValueMap map PRIMARY KEY
 * ((pk, serial), date) ) WITH CLUSTERING ORDER BY (date DESC) AND bloom_filter_fp_chance=0.010000 AND
 * caching='KEYS_ONLY' AND comment='' AND dclocal_read_repair_chance=0.100000 AND gc_grace_seconds=864000 AND
 * index_interval=128 AND read_repair_chance=0.000000 AND replicate_on_write='true' AND
 * populate_io_cache_on_flush='false' AND default_time_to_live=0 AND speculative_retry='99.0PERCENTILE' AND
 * memtable_flush_period_in_ms=0 AND compaction={'class': 'SizeTieredCompactionStrategy'} AND
 * compression={'sstable_compression': 'LZ4Compressor'};
 * 
 * @author asifbashar
 * 
 */
public class CassandraConnect
{
    private Cluster cluster = null;
    private Session session = null;
    private String[] hosts = { "127.0.0.1" };
    String queryToExecute = "";
    String keyspace = "";
    String table = "";
    String[] pkName = new String[1];
    String pkStr = null;
    String pktype = "";
    String username = "cassandra";
    String password = "cassandra";
    String query = null;
    int numItems = 1000;
    boolean avgonly = false;
    boolean singleQuery = false;
    ConsistencyLevel cl = ConsistencyLevel.LOCAL_QUORUM;
    String clStr = null;
    int numthreads = 10;
    String valuestoget = "";
    String outputFile = "";
    static FileOutputStream fos = null;
    String datatext = "As a non-relational database, Cassandra does not support joins or foreign keys, and consequently does not offer consistency in the ACID sense. For example, when moving money from account A to B the total in the accounts does not change. Cassandra supports atomicity and isolation at the row-level, but trades transactional isolation and atomicity for high availability and fast write performance. Cassandra writes are durable. ";
    int requestSize = 1000;
    long totalRetryCount = 0L;
    int numColumns = 1;
    HashMap<String, String> valueMap = new HashMap<String, String>();
    boolean readTest = false;
    boolean readWriteTest = false;
    boolean writeTest = false;
    boolean useDowngradePolicy = false;
    boolean storeOutputFile = false;
    String truststorePath = null;
    String truststorePassword = null;
    String keystorePath = null;
    String keystorePassword = null;
    int errorCount = 0;
    int maxRetryCount = 5;
    ArrayList<String[]> keylist = new ArrayList<String[]>();
    ArrayList<Long> perThreadDuration = new ArrayList<Long>();
    PoolingOptions poolingOptions = new PoolingOptions();

    int coreConnectionsPerHost = 16;
    int maxConnectionsPerHost = 16;
    int maxSimultaneousRequestsPerConnectionThreshold = 8;

    public static void main(String[] args) throws Exception
    {
        CassandraConnect client = new CassandraConnect();

        // TODO Auto-generated method stub
        for (int i = 0; i < args.length; i = i + 2)
        {
            if (args[i].equals("-h"))
            {
                client.setHosts(args[i + 1].split(","));
            }
            if (args[i].equals("-q"))
            {
                client.queryToExecute = args[i + 1];
            }
            if (args[i].equals("-keyspace"))
            {
                client.keyspace = args[i + 1];
            }

            if (args[i].equals("-table"))
            {
                client.table = args[i + 1];
            }

            if (args[i].equals("-pkname"))
            {
                client.pkStr = args[i + 1];
                client.pkName = client.pkStr.split(",");
            }
            if (args[i].equals("-query"))
            {
                client.query = args[i + 1];
            }
            if (args[i].equals("-username"))
            {
                client.username = args[i + 1];
            }
            if (args[i].equals("-password"))
            {
                client.password = args[i + 1];
            }
            if (args[i].equals("-cl"))
            {
                client.clStr = args[i + 1];
            }
            if (args[i].equals("-avgonly"))
            {
                if (args[i + 1] != null && args[i + 1].equalsIgnoreCase("true"))
                {
                    client.avgonly = true;
                }
            }
            if (args[i].equals("-numitems"))
            {
                client.numItems = Integer.parseInt(args[i + 1]);
            }

            if (args[i].equals("-pktype"))
            {
                client.pktype = args[i + 1];
            }
            if (args[i].equals("-numthreads"))
            {
                client.numthreads = Integer.parseInt(args[i + 1]);
            }
            if (args[i].equals("-valuestoget"))
            {
                client.valuestoget = args[i + 1];
            }
            if (args[i].equals("-outputfilelocation"))
            {
                client.outputFile = args[i + 1];
                try
                {
                    fos = new FileOutputStream(client.outputFile);
                }
                catch (FileNotFoundException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if (args[i].equals("-coreConnectionsPerHost"))
            {
                client.coreConnectionsPerHost = Integer.parseInt(args[i + 1]);
            }
            if (args[i].equals("-maxConnectionsPerHost"))
            {
                client.maxConnectionsPerHost = Integer.parseInt(args[i + 1]);
            }
            if (args[i].equals("-maxSimultaneousRequestsPerConnectionThreshold"))
            {
                client.maxSimultaneousRequestsPerConnectionThreshold = Integer.parseInt(args[i + 1]);
            }

            if (args[i].equals("-truststorePath"))
            {
                client.truststorePath = args[i + 1];
            }
            if (args[i].equals("-truststorePassword"))
            {
                client.truststorePassword = args[i + 1];
            }
            if (args[i].equals("-keystorePath"))
            {
                client.keystorePath = args[i + 1];
            }
            if (args[i].equals("-keystorePassword"))
            {
                client.keystorePassword = args[i + 1];
            }

            if (args[i].equals("-singleQuery"))
            {
                client.singleQuery = new Boolean(args[i + 1]);
            }

            if (args[i].equals("-readTest"))
            {
                client.readTest = new Boolean(args[i + 1]);
            }
            if (args[i].equals("-writeTest"))
            {
                client.writeTest = new Boolean(args[i + 1]);
            }
            if (args[i].equals("-readWriteTest"))
            {
                client.readWriteTest = new Boolean(args[i + 1]);
            }

            if (args[i].equals("-useDowngradePolicy"))
            {
                client.useDowngradePolicy = new Boolean(args[i + 1]);
            }

            if (args[i].equals("-storeOutputFile"))
            {
                client.storeOutputFile = new Boolean(args[i + 1]);
            }
            if (args[i].equals("-requestSize")) // in bytes
            {
                client.requestSize = Integer.parseInt(args[i + 1]);
            }
            if (args[i].equals("-numColumns")) // in bytes
            {
                client.numColumns = Integer.parseInt(args[i + 1]);
            }

            //
            // boolean readTest = false;
            // boolean writeTest = false;
            // boolean readwriteTest = false;
            // boolean failoverTest = false;

        }

        if (client.clStr != null)
        {

            client.cl = ConsistencyLevel.valueOf(client.clStr);

        }

        int columnSize = client.requestSize / client.numColumns;
        if (client.datatext.length() > columnSize)
        {
            client.datatext = client.datatext.substring(0, columnSize);
        }
        else
        {
            int datatextMultiple = columnSize / client.datatext.length();
            int partialTextToAdd = columnSize % client.datatext.length();
            if (datatextMultiple > 1)
            {
                StringBuffer fullDataText = new StringBuffer();
                for (int txtAddCnt = 0; txtAddCnt <= datatextMultiple; txtAddCnt++)
                {
                    fullDataText.append(client.datatext);
                }
                fullDataText.append(client.datatext.substring(0, partialTextToAdd));
                client.datatext = fullDataText.toString();
            }
        }

        for (int colCount = 0; colCount < client.numColumns; colCount++)
        {
            client.valueMap.put("column_" + colCount, client.datatext);
        }

        System.out.println("Average value column size for this test " + client.datatext.getBytes().length + " bytes" + " number of colmn-value pairs" + client.numColumns);
        System.out.println("Total retry done " + client.totalRetryCount);

        client.poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, client.coreConnectionsPerHost);
        client.poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, client.maxConnectionsPerHost);
        client.poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, client.maxSimultaneousRequestsPerConnectionThreshold);

        client.connect();
        if (client.readTest)
        {
            System.out.println("Started collecting item keys for ..");
            if (client.query == null || client.query.length() == 0)
            {
                client.query = "SELECT * FROM  " + client.keyspace + "." + client.table + " limit " + client.numItems;
            }

            Statement statement = new SimpleStatement(client.query);
            statement.setFetchSize(10);
            statement.setConsistencyLevel(client.cl);
            ResultSet rs = client.getSession().execute(statement);
            if (!client.singleQuery)
            {
                for (Row row : rs)
                {
                    String[] valueList = new String[client.pkName.length];
                    for (int i = 0; i < client.pkName.length; i++)
                    {
                        valueList[i] = row.getString(client.pkName[i]);
                    }
                    client.keylist.add(valueList);
                }
                System.out.println("Completed collecting item keys for  , now will measure performance");

                // client.performanceTest();

            }
        }
        setupThreads(client);

        client.getCluster().close();

        if (fos != null)
        {
            try
            {
                fos.close();
            }
            catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public static void setupThreads(CassandraConnect client)
    {
        long start = System.currentTimeMillis();
        PerformanceTest[] ptests = new PerformanceTest[client.numthreads];
        for (int i = 0; i < client.numthreads; i++)
        {
            PerformanceTest ptest = new PerformanceTest(client, "Thread " + i);
            System.out.println("String thread " + i);
            ptest.start();
            ptests[i] = ptest;

        }
        for (int i = 0; i < client.numthreads; i++)
        {

            try
            {
                ptests[i].join();
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
        long totalDuration = 0L;
        long totalRequests = client.numItems;
        for (int i = 0; i < client.perThreadDuration.size(); i++)
        {
            totalDuration = totalDuration + client.perThreadDuration.get(i);
        }
        if (totalRequests > 0)
        {
            long average = totalDuration / totalRequests;
            System.out.println("Total Requests " + totalRequests);
            System.out.println("Average time per requests " + average + " milliseconds");
        }
        long end = System.currentTimeMillis();
        System.out.println("Total duration for tests " + (end - start));
    }

    // public static void failoverTest(CassandraConnect client) throws IOException
    // {
    // BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("dataWritten.txt", true));
    // for (long x = 0; x < client.numItems; x++)
    // {
    // UUID uuid = com.datastax.driver.core.utils.UUIDs.timeBased();
    // long time = System.currentTimeMillis();
    // Statement statement = QueryBuilder
    // .insertInto(client.keyspace, client.table)
    // .value("pk", uuid)
    // .value("serial", serial)
    // .value("date", time)
    // .value("value",
    // time
    // +
    // "As a non-relational database, Cassandra does not support joins or foreign keys, and consequently does not offer
    // consistency in the ACID sense. For example, when moving money from account A to B the total in the accounts does
    // not change. Cassandra supports atomicity and isolation at the row-level, but trades transactional isolation and
    // atomicity for high availability and fast write performance. Cassandra writes are durable. "
    // + serial);
    // bos.write((uuid.toString() + "," + serial + "," + time + "\n").getBytes());
    // statement.setFetchSize(10);
    // statement.setConsistencyLevel(client.cl);
    // ResultSet rs = client.getSession().execute(statement);
    //
    // Statement statementRead = QueryBuilder.select().all().from(client.keyspace,
    // client.table).where(QueryBuilder.eq("pk", uuid)).and(QueryBuilder.eq("serial",
    // serial)).and(QueryBuilder.eq("date", time));
    //
    // statementRead.setFetchSize(10);
    // statementRead.setConsistencyLevel(client.cl);
    // ResultSet rsRead = client.getSession().execute(statementRead);
    //
    // if (rsRead.iterator().hasNext())
    // {
    // int cRowCount = 0;
    // for (Row row : rsRead)
    // {
    // cRowCount++;
    // }
    // if (cRowCount == 0)
    // {
    // System.out.println("No row found for serial " + serial);
    // }
    //
    // }
    // else
    //
    // {
    // System.out.println("No row found for serial " + serial);
    // }
    //
    // if (x % 200 == 0)
    // {
    // System.out.println("Written and read " + x + "rows, current serial " + serial);
    // }
    // serial++;
    // }
    // bos.close();
    //
    // }

    public static class PerformanceTest extends Thread
    {
        CassandraConnect cn = null;
        String threadName = "";

        public PerformanceTest(CassandraConnect cassconn, String tName)
        {
            cn = cassconn;
            threadName = tName;
        }

        @Override
        public void run()
        {
            if (threadName != null)
            {
                this.currentThread().setName(threadName);
            }

            if (cn.readTest)
            {
                try
                {
                    cn.readTest(threadName);
                }
                catch (IOException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if (cn.writeTest)
            {
                try
                {
                    cn.writeTest(threadName);
                }
                catch (IOException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if (cn.readWriteTest)
            {
                try
                {
                    cn.readWriteTest(threadName);
                }
                catch (IOException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if (cn.readTest)
            {
                cn.performanceReadTest(threadName);
            }
        }
    }

    /*
     * 
     */
    public void writeTest(String runId) throws IOException
    {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("dataWritten.txt", true));
        long itemsPerThread = this.numItems / this.numthreads;
        long recSerial = 0L;
        for (long x = 0; x < itemsPerThread; x++)
        {
            UUID uuid = com.datastax.driver.core.utils.UUIDs.timeBased();
            long time = System.currentTimeMillis();
            Statement statement = QueryBuilder.insertInto(this.keyspace, this.table).value("pk", uuid).value("serial", recSerial).value("date", time).value("value", time + datatext + recSerial);
            bos.write((uuid.toString() + "," + recSerial + "," + time + "\n").getBytes());
            statement.setFetchSize(10);
            statement.setConsistencyLevel(this.cl);
            boolean writeValue = false;
            int retryCount = 0;
            while (!writeValue)
            {
                try
                {
                    retryCount++;
                    ResultSet rs = getSession().execute(statement);

                    writeValue = true;

                }
                catch (Exception e)
                {

                }
                if (retryCount > maxRetryCount)
                {
                    break;
                }
                if (!writeValue)
                {
                    totalRetryCount++;
                }
            }
            if (x % 200 == 0)
            {
                System.out.println("Written " + x + "rows, current serial " + recSerial);
            }
            recSerial++;
        }
        bos.close();
    }

    public void readTest(String runId) throws IOException
    {

        BufferedReader bfr = new BufferedReader(new InputStreamReader(new FileInputStream("dataWritten.txt")));
        String strLine = "";
        long countOfRead = 0L;
        while ((strLine = bfr.readLine()) != null)
        {
            {

                String[] values = strLine.split(",");
                UUID uuid = UUID.fromString(values[0]);
                long serial = Long.parseLong(values[1]);

                long time = Long.parseLong(values[2]);
                Statement statement = QueryBuilder.select().all().from(this.keyspace, this.table).where(QueryBuilder.eq("pk", uuid)).and(QueryBuilder.eq("serial", serial)).and(QueryBuilder.eq("date", time));

                statement.setFetchSize(10);
                statement.setConsistencyLevel(this.cl);
                boolean readValue = false;
                int retryCount = 0;
                while (!readValue)
                {
                    try
                    {
                        retryCount++;
                        ResultSet rs = getSession().execute(statement);

                        if (rs.iterator().hasNext())
                        {
                            for (Row row : rs)
                            {
                                countOfRead = countOfRead + 1;
                                // System.out.println("read serial " + serial);
                                // System.out.println("Found UUID: " + row.getUUID("pk"));
                            }

                        }
                        else
                        {
                            System.out.println("Now row found for serial " + serial);
                        }
                        readValue = true;

                    }
                    catch (Exception e)
                    {

                    }
                    if (retryCount > maxRetryCount)
                    {
                        break;
                    }
                    if (!readValue)
                    {
                        totalRetryCount++;
                    }
                }

                if ((countOfRead % 200) == 0)
                {
                    System.out.println(" total rows read " + countOfRead);
                }

            }

        }
        bfr.close();
        System.out.println("All read tests completed, total rows read " + countOfRead);
    }

    public void readWriteTest(String runId) throws IOException
    {
        Logger logger = Logger.getLogger("performanceOutput");
        long recSerial = 0L;
        long itemsPerThread = this.numItems / this.numthreads;
        BufferedOutputStream bos = null;

        long start = System.currentTimeMillis();
        if (storeOutputFile)
        {
            bos = new BufferedOutputStream(new FileOutputStream("dataWritten.txt", true));
        }
        for (long x = 0; x < itemsPerThread; x++)
        {
            UUID uuid = com.datastax.driver.core.utils.UUIDs.timeBased();
            long time = System.currentTimeMillis();

            Statement statement = QueryBuilder.insertInto(keyspace, table).value("pk", uuid).value("serial", recSerial).value("date", time).value("value", time + datatext + recSerial).value("columnValueMap", valueMap);

            if (storeOutputFile)
            {
                bos.write((uuid.toString() + "," + recSerial + "," + time + "\n").getBytes());
            }
            // statement.setFetchSize(10);
            statement.setConsistencyLevel(cl);
            boolean writeValue = false;
            int retryCount = 0;
            while (!writeValue)
            {
                try
                {
                    retryCount++;
                    ResultSet rs = getSession().execute(statement);
                    writeValue = true;

                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                if (retryCount > maxRetryCount)
                {
                    break;
                }
                if (!writeValue)
                {
                    totalRetryCount++;
                }
            }
            Statement statementRead = QueryBuilder.select().all().from(keyspace, table).where(QueryBuilder.eq("pk", uuid)).and(QueryBuilder.eq("serial", recSerial)).and(QueryBuilder.eq("date", time));

            // statementRead.setFetchSize(10);
            statementRead.setConsistencyLevel(cl);
            boolean readValue = false;
            retryCount = 0;
            while (!readValue)
            {
                try
                {
                    retryCount++;

                    ResultSet rsRead = getSession().execute(statementRead);

                    if (rsRead.iterator().hasNext())
                    {
                        int cRowCount = 0;
                        for (Row row : rsRead)
                        {
                            cRowCount++;
                        }
                        if (cRowCount == 0)
                        {
                            logger.log(Level.INFO, "No row found for serial " + recSerial);
                        }

                    }
                    else

                    {
                        logger.log(Level.INFO, "No row found for serial " + recSerial);
                    }

                    readValue = true;

                }
                catch (Exception e)
                {

                }
                if (retryCount > maxRetryCount)
                {
                    break;
                }
                if (!readValue)
                {
                    totalRetryCount++;
                }
            }

            if (x % 1000 == 0)
            {
                logger.log(Level.INFO, "Written and read " + x + "rows, current serial " + recSerial);
            }
            recSerial++;
        }
        if (storeOutputFile)
        {
            bos.close();
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        perThreadDuration.add(duration);
        System.out.println("total time for  " + itemsPerThread + " requests  is " + (duration) + " milliseconds");
        logger.log(Level.INFO, "total time for  " + itemsPerThread + " requests  is " + (duration) + " milliseconds");
        System.out.println("average time per request " + (duration) / itemsPerThread + " milliseconds");
        logger.log(Level.INFO, "average time per request " + (duration) / itemsPerThread + " milliseconds");

    }

    public void performanceReadTest(String runId)
    {

        String[] valuesToGetArray = new String[0];
        if (valuestoget != null && valuestoget.length() > 0)
        {
            valuesToGetArray = valuestoget.split(",");
        }

        int keylistSize = keylist.size();

        long singleQueryStart = 0L;
        long singleQueryEnd = 0L;

        long start = System.currentTimeMillis();
        int keyIndex = 0;
        if (keylistSize > 0)
        {
            for (int i = 0; i < this.numItems; i++)
            {

                String quote = "";
                if (pktype != null && pktype.equals("char"))
                {
                    quote = "'";
                }

                String[] keyvalues = keylist.get(keyIndex);
                query = "SELECT * FROM  " + keyspace + "." + table;

                for (int c = 0; c < this.pkName.length; c++)
                {
                    if (c == 0)
                    {
                        query = query + " where ";
                    }

                    query = query + "  " + pkName[c] + " = " + quote + keyvalues[c] + quote;

                    if (c < (this.pkName.length - 1))
                    {
                        query = query + " AND  ";
                    }
                }

                SimpleStatement statement = new SimpleStatement(query);
                statement.setConsistencyLevel(cl);
                if (!avgonly)
                {
                    singleQueryStart = System.currentTimeMillis();
                }
                ResultSet rs = null;
                try
                {
                    rs = getSession().execute(statement);
                }
                catch (UnavailableException ue)
                {

                }
                Iterator<Row> rowItr = rs.iterator();
                while (rowItr.hasNext())
                {
                    Row r = rowItr.next();
                    if (valuesToGetArray.length > 0)
                    {
                        for (int k = 0; k < valuesToGetArray.length; k++)
                        {
                            ColumnDefinitions cdef = r.getColumnDefinitions();
                            DataType dt = cdef.getType(valuesToGetArray[k]);
                            if (dt.getName().toString().equals("blob"))
                            {
                                ByteBuffer bf = r.getBytes(valuesToGetArray[k]);
                                if (fos != null)
                                {
                                    try
                                    {
                                        fos.write((valuesToGetArray[k] + ":").getBytes());
                                        fos.write(bf.array());
                                        fos.write("\n".getBytes());
                                    }
                                    catch (IOException e)
                                    {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                }
                            }
                            else if (dt.getName().toString().equals("varchar"))
                            {
                                String str = r.getString(valuesToGetArray[k]);
                                if (fos != null)
                                {
                                    try
                                    {
                                        fos.write((valuesToGetArray[k] + ":").getBytes());
                                        fos.write(str.getBytes());
                                        fos.write("\n".getBytes());
                                    }
                                    catch (IOException e)
                                    {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        ColumnDefinitions cdef = r.getColumnDefinitions();

                        for (int k = 0; k < cdef.size(); k++)
                        {
                            DataType dt = cdef.getType(k);
                            if (dt.getName().toString().equals("blob"))
                            {
                                ByteBuffer bf = r.getBytes(k);
                                if (fos != null)
                                {
                                    try
                                    {
                                        fos.write((cdef.getName(k) + ":").getBytes());
                                        fos.write(bf.array());
                                        fos.write("\n".getBytes());
                                    }
                                    catch (IOException e)
                                    {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                }
                            }
                            else if (dt.getName().toString().equals("varchar"))
                            {
                                String str = r.getString(k);
                                if (fos != null)
                                {
                                    try
                                    {
                                        fos.write((cdef.getName(k) + ":").getBytes());
                                        fos.write(str.getBytes());
                                        fos.write("\n".getBytes());
                                    }
                                    catch (IOException e)
                                    {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                }
                            }

                        }
                    }
                }
                if (!avgonly)
                {
                    singleQueryEnd = System.currentTimeMillis();
                    System.out.println(runId + " : To find key \"" + this.pkStr + "\"  time spent " + (singleQueryEnd - singleQueryStart) + " milliseconds");
                }

                if (keyIndex == (keylistSize - 1))
                {
                    keyIndex = 0;
                }
                else
                {
                    keyIndex++;
                }

            }
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        perThreadDuration.add(duration);
        System.out.println("total time for  " + this.numItems + " requests  is " + (duration) + " milliseconds");
        System.out.println("average time per request " + (duration) / this.numItems + " milliseconds");

    }

    public void querySchema(String query)
    {

        // setting the consistency level on a statement
        if (query == null || query.length() == 0)
        {
            query = "SELECT * FROM  Excelsior.Test";
        }
        Statement statement = new SimpleStatement(query);
        // statement.setFetchSize(10);
        statement.setConsistencyLevel(ConsistencyLevel.ONE);
        ResultSet rs = getSession().execute(statement);
        ColumnDefinitions cd = rs.getColumnDefinitions();

        for (Row row : rs)
        {
            for (int i = 0; i < cd.size(); i++)
            {
                DataType type = cd.getType(i);
                if (type.getName().toString().equals("blob"))
                {
                    System.out.printf("%s %s", type.getName().toString(), row.getBytes(cd.getName(i)));
                }
                else if (type.getName().toString().equals("varchar"))
                {
                    System.out.printf("%s %s", type.getName().toString(), row.getString(cd.getName(i)));
                }
            }
            System.out.printf("\n");

        }
        // setting the fetch size on a statement
        // the following SELECT statement returns over 5K results
    }

    public void querySchema2(String keyspace, String table)
    {
        Select select = QueryBuilder.select().all().from(keyspace, table);

        select.limit(10);
        ResultSet rs = session.execute(select);
        ColumnDefinitions cd = rs.getColumnDefinitions();
        for (Row row : rs)
        {
            for (int i = 0; i < cd.size(); i++)
            {
                DataType type = cd.getType(i);
                if (type.getName().toString().equals("blob"))
                {
                    System.out.printf("%s %s", type.getName().toString(), row.getBytes(cd.getName(i)));
                }
                else if (type.getName().toString().equals("varchar"))
                {
                    System.out.printf("%s %s", type.getName().toString(), row.getString(cd.getName(i)));
                }
            }
            System.out.printf("\n");

        }

    }

    public void createCustomSchema() throws UnsupportedEncodingException
    {
        session.execute("drop keyspace iphonesys_test;");
        session.execute("CREATE KEYSPACE iphonesys_test WITH replication " + "=  {'class' : 'NetworkTopologyStrategy', 'NC1':3};");
        session.execute("CREATE TABLE iphonesys_test.\"TestColumnFamily2\" (" + "key blob," + "column1 blob," + "value blob," + "PRIMARY KEY (key, column1)" + "  ); ");

        Statement insert = QueryBuilder.insertInto("iphonesys_test", "\"TestColumnFamily2\"").value("key", ByteBuffer.wrap(UUID.randomUUID().toString().getBytes("UTF-8")))
                .value("column1", ByteBuffer.wrap("Column1Values".getBytes("UTF-8"))).value("value", ByteBuffer.wrap("TESTVALUE100000".getBytes("UTF-8"))).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        session.execute(insert);
    }

    public Cluster getCluster()
    {
        return cluster;
    }

    // public void createSchema()
    // {
    // session.execute("CREATE KEYSPACE simplex WITH replication " +
    // "= {'class':'NetworkTopologyStrategy', 'replication_factor':3};");
    // }
    //
    public void setCluster(Cluster cluster)
    {
        this.cluster = cluster;
    }

    public Session getSession()
    {
        return session;
    }

    public void setSession(Session session)
    {
        this.session = session;
    }

    public String[] getHosts()
    {
        return hosts;
    }

    public void setHosts(String[] hostArray)
    {
        hosts = hostArray;
    }

    public CassandraConnect()
    {

    }

    public static class ConnectionOptions
    {

        public static final int DEFAULT_MAX_CONNECTIONS = 8;
        public static final int DEFAULT_MAX_SIMULTANEOUS_REQUESTS_PER_CONNECTION = 100;

        private String clusterName;
        private String[] nodes;
        private int port;
        private String username;
        private String password;
        private int maxConnections = DEFAULT_MAX_CONNECTIONS;
        private int maxSimultaneousRequestsPerConnection = DEFAULT_MAX_SIMULTANEOUS_REQUESTS_PER_CONNECTION;
        private String retryPolicyClass;

        public ConnectionOptions()
        {

        }

        public ConnectionOptions(String clusterName, String[] nodes, int port, String username, String password, int maxConnections, int maxSimultaneousRequestsPerConnection, String retryPolicyClass)
        {
            this.clusterName = clusterName;
            this.nodes = nodes;
            this.port = port;
            this.username = username;
            this.password = password;
            this.maxConnections = maxConnections;
            this.maxSimultaneousRequestsPerConnection = maxSimultaneousRequestsPerConnection;
            this.retryPolicyClass = retryPolicyClass;
        }

        public String getClusterName()
        {
            return clusterName;
        }

        public void setClusterName(String clusterName)
        {
            this.clusterName = clusterName;
        }

        public String[] getNodes()
        {
            return nodes;
        }

        public void setNodes(String[] nodes)
        {
            this.nodes = nodes;
        }

        public int getPort()
        {
            return port;
        }

        public void setPort(int port)
        {
            this.port = port;
        }

        public String getUsername()
        {
            return username;
        }

        public void setUsername(String username)
        {
            this.username = username;
        }

        public String getPassword()
        {
            return password;
        }

        public void setPassword(String password)
        {
            this.password = password;
        }

        public int getMaxConnections()
        {
            return maxConnections;
        }

        public void setMaxConnections(int maxConnections)
        {
            this.maxConnections = maxConnections;
        }

        public int getMaxSimultaneousRequestsPerConnection()
        {
            return maxSimultaneousRequestsPerConnection;
        }

        public void setMaxSimultaneousRequestsPerConnection(int maxSimultaneousRequestsPerConnection)
        {
            this.maxSimultaneousRequestsPerConnection = maxSimultaneousRequestsPerConnection;
        }

        /**
         * this method will attempt to create an instance of retry policy class and return it
         * 
         * @return an instance of specified retryPolicyClass. a null will be returned if the instance of
         *         retryPolicyClass cannot be constructed
         */
        public RetryPolicy getRetryPolicyInstance()
        {
            try
            {
                if (retryPolicyClass == null)
                {
                    return null;
                }
                return (RetryPolicy) Class.forName(retryPolicyClass).getField("INSTANCE").get(null);
            }
            catch (Exception ex)
            {

            }
            return null;
        }

        public void setRetryPolicyClass(String retryPolicyClass)
        {
            this.retryPolicyClass = retryPolicyClass;
        }

        public String getRetryPolicyClass()
        {
            return this.retryPolicyClass;
        }

    }

    private synchronized static Cluster getCluster(final ConnectionOptions connectionOptions)
    {
        // String clusterId = Arrays.asList(nodes).toString();

        PoolingOptions poolingOptions = new PoolingOptions();

        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, connectionOptions.getMaxConnections());
        poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, connectionOptions.getMaxSimultaneousRequestsPerConnection());
        Cluster.Builder builder = Cluster.builder().withClusterName(connectionOptions.getClusterName()).addContactPoints(connectionOptions.getNodes()).withPort(connectionOptions.getPort())
                .withCredentials(connectionOptions.getUsername(), connectionOptions.getPassword()).withReconnectionPolicy(new ConstantReconnectionPolicy(100L)).withSocketOptions(new SocketOptions().setKeepAlive(true))
                .withPoolingOptions(poolingOptions);

        RetryPolicy retryPolicyInstance = connectionOptions.getRetryPolicyInstance();
        if (retryPolicyInstance != null)
        {

            builder = builder.withRetryPolicy(connectionOptions.getRetryPolicyInstance());
        }
        else
        {

        }
        // .withoutJMXReporting()
        // .withSSL() // uncomment if using client to node encryption

        final Cluster cluster = builder.build();

        return cluster;
    }

    private synchronized static Cluster getCluster(final String clusterName, String[] nodes, int port, String username, String password, int maxConnections, int maxSimultaneousRequestsPerConnection)
    {
        // String clusterId = Arrays.asList(nodes).toString();

        PoolingOptions poolingOptions = new PoolingOptions();

        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, maxSimultaneousRequestsPerConnection);
        final Cluster cluster = Cluster.builder().withClusterName(clusterName).addContactPoints(nodes).withPort(port).withCredentials(username, password).withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                .withSocketOptions(new SocketOptions().setKeepAlive(true)).withPoolingOptions(poolingOptions)
                // .withoutJMXReporting()
                // .withSSL() // uncomment if using client to node encryption
                .build();
        // cluster =
        // Cluster.builder().withClusterName(clusterName).addContactPoints(nodes).withPort(port).withCredentials(username,
        // password).withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
        // .withSocketOptions(new
        // SocketOptions().setKeepAlive(true).setConnectTimeoutMillis(60000).setReadTimeoutMillis(60000).setTcpNoDelay(true)).withPoolingOptions(poolingOptions).build();

        return cluster;
    }

    public void connect() throws Exception
    {

        com.datastax.driver.core.Cluster.Builder builder = Cluster.builder();
        if (useDowngradePolicy)
        {
            builder.addContactPoints(this.getHosts()).withPort(9042).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE).withReconnectionPolicy(new ExponentialReconnectionPolicy(100L, 60000L))
                    .withCredentials(username, password).withPoolingOptions(poolingOptions);
        }
        else
        {
            builder.addContactPoints(this.getHosts()).withPort(9042).withReconnectionPolicy(new ConstantReconnectionPolicy(100L)).withCredentials(username, password).withPoolingOptions(poolingOptions);
        }
        if (this.truststorePath != null)
        {
            builder.withSSL(CassandraConnect.getSSLOptions(this.truststorePath, this.truststorePassword, this.keystorePath, this.keystorePassword));
        }
        // builder.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("PR1")));
        this.setCluster(builder.build());

        this.setSession(cluster.connect());

    }

    public static SSLOptions getSSLOptions(String truststorePath, String truststorePassword, String keystorePath, String keystorePassword) throws Exception
    {

        SSLContext context = getSSLContext(truststorePath, truststorePassword, keystorePath, keystorePassword);

        // Default cipher suites supported by C*
        String[] cipherSuites = { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" };

        SSLOptions sslOptions = new SSLOptions(context, cipherSuites);
        return sslOptions;
    }

    private static SSLContext getSSLContext(String truststorePath, String truststorePassword, String keystorePath, String keystorePassword) throws Exception
    {
        FileInputStream tsf = new FileInputStream(truststorePath);

        SSLContext ctx = SSLContext.getInstance("SSL");

        KeyStore ts = KeyStore.getInstance("JKS");
        ts.load(tsf, truststorePassword.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        if (keystorePath != null)
        {
            FileInputStream ksf = new FileInputStream(keystorePath);

            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(ksf, keystorePassword.toCharArray());

            kmf.init(ks, keystorePassword.toCharArray());
            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        }
        else
        {
            ctx.init(null, tmf.getTrustManagers(), new SecureRandom());
        }

        return ctx;
    }

}
