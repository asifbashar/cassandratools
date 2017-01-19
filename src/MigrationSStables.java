import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.yaml.snakeyaml.Yaml;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

/**
 * 
 * @author asifbashar
 *
 */
public class MigrationSStables
{

    public static void main(String[] args) throws Exception
    {
        String rootPath = "";
        if (args.length > 0)
        {
            rootPath = args[0];
        }
        Yaml yaml = new Yaml();
        // Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) yaml.load(new FileInputStream(new
        // File(rootPath + config.)));

        Properties prop = new Properties();
        // prop.load(fis);

        migrateSSTables(prop);
        // fis.close();

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

    public static void migrateSSTables(Properties prop) throws Exception
    {

        String srcClusterYamlPath = prop.getProperty("sourceYamlPath");
        String keyspacesToExport = prop.getProperty("keyspacesToExport");
        String snapshotName = prop.getProperty("snapshotName");
        String keyspacesInDestinations = prop.getProperty("keyspacesInDestinations");
        Yaml yaml = new Yaml();

        Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) yaml.load(new FileInputStream(new File(srcClusterYamlPath)));

        ArrayList<String> dataDirs = (ArrayList) map.get("data_file_directories");
        String sourceJmxPort = prop.getProperty("sourceJmxPort");
        String sourceNativePort = "9042";// (String) map.get("native_transport_port");
        String sourceUsername = prop.getProperty("sourceUsername");
        String sourcePassword = prop.getProperty("sourcePassword");
        String sourceSeedNodes = (String) ((Map) ((ArrayList) ((Map) ((ArrayList) map.get("seed_provider")).get(0)).get("parameters")).get(0)).get("seeds");
        // (String)
        // ((Map)
        // ((ArrayList)
        // map.get("seed_provider"));//.get("parameters")).get("seeds");
        String sourceClientEncryptionEnabled = (String) ((Map) map.get("client_encryption_options")).get("enabled");
        String sourceTruststorePath = (String) ((Map) map.get("client_encryption_options")).get("truststore");
        String sourceTruststorePassword = (String) ((Map) map.get("client_encryption_options")).get("truststore_password");
        String sourceKeystorePath = (String) ((Map) map.get("client_encryption_options")).get("keystore");
        String sourceKeystorePassword = (String) ((Map) map.get("client_encryption_options")).get("keystore_password");

        String destinationStoragePort = prop.getProperty("destinationStoragePort");
        String destinationNativePort = prop.getProperty("destinationNativePort");
        String destinationSeedNodes = prop.getProperty("destinationSeedNodes");
        String destinationTruststorePath = prop.getProperty("destinationTruststorePath");
        String destinationTruststorePassword = prop.getProperty("destinationTruststorePassword");
        String destinationUsername = prop.getProperty("destinationUsername");
        String destinationPassword = prop.getProperty("destinationPassword");

        String destSslEnabled = prop.getProperty("destinationSslEnabled");

        String dropRecreateDestinationTable = prop.getProperty("dropRecreateDestinationTable");

        com.datastax.driver.core.Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints(sourceSeedNodes.split(",")).withPort(Integer.parseInt(sourceNativePort)).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(100L, 60000L)).withCredentials(sourceUsername, sourcePassword);
        if (sourceClientEncryptionEnabled.equals("true"))
        {
            builder.withSSL(getSSLOptions(sourceTruststorePath, sourceTruststorePassword, sourceKeystorePath, sourceKeystorePassword));
        }
        Cluster srcCluster = builder.build();
        Session srcSession = srcCluster.connect();

        com.datastax.driver.core.Cluster.Builder builderDest = Cluster.builder();
        builderDest.addContactPoints(destinationSeedNodes.split(",")).withPort(Integer.parseInt(destinationNativePort)).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(100L, 60000L)).withCredentials(destinationUsername, destinationPassword);
        if (destSslEnabled.equals("true"))
        {
            builderDest.withSSL(getSSLOptions(prop.getProperty("destinationClusterTruststorePathPath"), prop.getProperty("destinationTruststorePassword"), prop.getProperty("destinationKeystorePath"),
                    prop.getProperty("destinationKeystorePassword")));
        }
        Cluster destCluster = builderDest.build();
        Session destSession = destCluster.connect();

        String[] srcKeyspaceArr = keyspacesToExport.split(",");
        String[] keyspacesInDestinationsArr = keyspacesInDestinations.split(",");
        int keyspaceIndex = 0;
        for (String srcKeyspaceName : srcKeyspaceArr)
        {
            Metadata metadata = srcSession.getCluster().getMetadata();
            Collection<UserType> userTypes = metadata.getKeyspace(srcKeyspaceName).getUserTypes();
            for (UserType userType : userTypes)
            {
                String cqlUserType = userType.asCQLQuery();
                cqlUserType = cqlUserType.replace(srcKeyspaceName, keyspacesInDestinationsArr[keyspaceIndex]);
                cqlUserType = "drop type " + keyspacesInDestinationsArr[keyspaceIndex] + "." + userType.getTypeName() + ";" + cqlUserType;
                destSession.execute(cqlUserType);
            }

            Collection<TableMetadata> tableMetadataList = metadata.getKeyspace(srcKeyspaceName).getTables();

            for (TableMetadata tmeta : tableMetadataList)
            {

                ResultSet srcKeyspaceDefRs = srcSession.execute("select  cf_id  from system.schema_columnfamilies where keyspace_name='" + srcKeyspaceName + "' and columnfamily_name='" + tmeta.getName() + "';");
                UUID cf_id = null;
                Iterator<Row> rowItr = srcKeyspaceDefRs.iterator();
                while (rowItr.hasNext())
                {
                    Row srcCFDefRow = rowItr.next();

                    cf_id = srcCFDefRow.getUUID("cf_id");
                    break;

                }

                String cqlTable = tmeta.asCQLQuery();
                String tableToCreate = cqlTable.replace(srcKeyspaceName, keyspacesInDestinationsArr[keyspaceIndex]);
                tableToCreate = "drop table " + keyspacesInDestinationsArr[keyspaceIndex] + "." + tmeta.getName() + ";" + tableToCreate;
                destSession.execute(tableToCreate);
                System.out.println(cqlTable);
            }
            keyspaceIndex++;
        }

        // keyspace_name | columnfamily_name | bloom_filter_fp_chance | caching | cf_id ")
        // String dataDirs = prop.getProperty("dataDirs");
        // String[] dataDirList = dataDirs.split(",");

        // for( String dataDir:dataDirList)
        // {
        // File cDir = new File(dataDir);
        // File[] files = cDir.listFiles();
        // // Arrays.sort(files, cDir);
        // }
    }

}
