import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * create TABLE test.tutorial ( pk timeuuid, serial bigint, date timestamp, value text, columnValueMap map PRIMARY KEY
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
public class CassandraSelectAll
{
    Logger logger = Logger.getLogger(this.getClass().getName());
    private Cluster cluster = null;
    private Session[] session = null;
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
    int numSessions = 1;
    String truststorePath = null;
    String truststorePassword = null;
    String keystorePath = null;
    String keystorePassword = null;
    int errorCount = 0;
    int maxRetryCount = 5;
    ArrayList<String[]> keylist = new ArrayList<String[]>();
    ArrayList<Long> perThreadDuration = new ArrayList<Long>();
    PoolingOptions poolingOptions = new PoolingOptions();
    int cSessionNo = 0;

    int coreConnectionsPerHost = 16;
    int maxConnectionsPerHost = 16;
    int maxSimultaneousRequestsPerConnectionThreshold = 8;

    public static void main(String[] args) throws Exception
    {
        CassandraSelectAll client = new CassandraSelectAll();

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
            if (args[i].equals("-numSessions")) // in bytes
            {
                client.numSessions = Integer.parseInt(args[i + 1]);
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

        client.poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, client.coreConnectionsPerHost);
        client.poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, client.maxConnectionsPerHost);

        client.connect();
        client.readAll();
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

    public void readAll() throws IOException
    {
        Metadata metadata = this.getCluster().getMetadata();
        Set<TokenRange> tokenRanges = metadata.getTokenRanges();
        Iterator<TokenRange> tokenItr = tokenRanges.iterator();
        int numColumns = -1;
        while (tokenItr.hasNext())
        {
            TokenRange tokenRange = tokenItr.next();

            Token stToken = tokenRange.getStart();
            Token endToken = tokenRange.getEnd();
            logger.info("Processing token range: startToken " + stToken + " , endToken " + endToken);
            Statement select = QueryBuilder.select().all().from(keyspace, table).where(QueryBuilder.gte(QueryBuilder.token(this.pkName), stToken.getValue()))
                    .and(QueryBuilder.lt(QueryBuilder.token(this.pkName), endToken.getValue()));
            select.setFetchSize(50);
            ResultSet resultSet = this.getSession().execute(select);
            Iterator<Row> iterator = resultSet.iterator();
            while (!resultSet.isFullyFetched())
            {
                resultSet.fetchMoreResults();
                Row row = iterator.next();
                if (numColumns == -1)
                {
                    numColumns = row.getColumnDefinitions().size();
                }
                // Enter logic for processing row.
                String output = "";
                for (int i = 0; i < numColumns; i++)
                {
                    if (row.getObject(i) != null)
                    {
                        output = output + row.getObject(i).toString();
                    }
                }
                logger.info(output);
            }
        }

    }

    public Session getSession()
    {
        if (cSessionNo == (session.length - 1))
        {
            cSessionNo = 0;
        }
        else
        {
            cSessionNo = cSessionNo + 1;
        }
        return session[cSessionNo];
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

    public void setSession(Session[] session)
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

    public CassandraSelectAll()
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
        // poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL,
        // connectionOptions.getMaxSimultaneousRequestsPerConnection());
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

        Session[] sessions = new Session[this.numSessions];
        for (int sc = 0; sc < this.numSessions; sc++)
        {
            sessions[sc] = cluster.connect();
        }
        this.setSession(sessions);

    }

    public static SSLOptions getSSLOptions(String truststorePath, String truststorePassword, String keystorePath, String keystorePassword) throws Exception
    {

        SSLContext context = getSSLContext(truststorePath, truststorePassword, keystorePath, keystorePassword);

        // Default cipher suites supported by C*
        String[] cipherSuites = { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" };
        com.datastax.driver.core.JdkSSLOptions jdkSSLOption = com.datastax.driver.core.JdkSSLOptions.builder().withCipherSuites(cipherSuites).withSSLContext(context).build();

        return jdkSSLOption;
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
