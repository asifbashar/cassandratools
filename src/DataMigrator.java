import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.yaml.snakeyaml.Yaml;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

public class DataMigrator
{

    private static Session session = null;

    public static void main(String[] args) throws Exception
    {
        HashMap props = new HashMap();

        if (args.length > 0)
        {
            if (new File(args[0]).exists())
            {
                FileInputStream fis = new FileInputStream(args[0]);
                Properties defaultProps = new Properties();
                defaultProps.load(fis);
                fis.close();
                props.putAll(defaultProps);
            }
            for (int i = 0; i < args.length; i++)
            {
                String[] prop = args[i].split("=");
                if (prop.length > 1)
                {
                    props.put(prop[0], prop[1]);
                }
            }
        }

        setupConfiguration(props);

        DataMigrator fs = new DataMigrator(props);

        try
        {
            setupCassandraConnection(props);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        try
        {
            fs.startServer(props);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void setupConfiguration(HashMap props) throws Exception
    {
        Map env = System.getenv();
        Object deployMode = env.get("C_DEPLOY_MODE");
        Object cassHome = env.get("CASS_HOME");
        if (cassHome == null || !(new File(cassHome.toString()).exists()))
        {
            throw new Exception("CASS_HOME is not set to a valid cassandra or dse installation directory path.");
        }
        Object homeDir = env.get("HOME_DIR");
        if (homeDir == null || !(new File(homeDir.toString()).exists()))
        {
            throw new Exception("HOME_DIR is not set to a existing directory path.");
        }

        System.out.println("HOME_DIR: " + homeDir + " CASS_HOME: " + cassHome);
        String yamlPath = cassHome + File.separator + "resources/cassandra/conf/cassandra.yaml";
        if (deployMode != null && deployMode.equals("apache_cassandra"))
        {
            yamlPath = cassHome + File.separator + "conf/cassandra.yaml";
        }

        Yaml yaml = new Yaml();

        Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) yaml.load(new FileInputStream(new File(yamlPath)));

        String sourceClientEncryptionEnabled = (((Map) map.get("client_encryption_options")).get("enabled")).toString();
        String sourceTruststorePath = (String) ((Map) map.get("client_encryption_options")).get("truststore");
        String sourceTruststorePassword = (String) ((Map) map.get("client_encryption_options")).get("truststore_password");
        String sourceKeystorePath = (String) ((Map) map.get("client_encryption_options")).get("keystore");
        String sourceKeystorePassword = (String) ((Map) map.get("client_encryption_options")).get("keystore_password");

        props.put("keystorePath", sourceKeystorePath);
        props.put("keystorePassword", sourceKeystorePassword);
        props.put("truststorePath", sourceTruststorePath);
        props.put("truststorePassword", sourceTruststorePassword);
        props.put("cassandraSSL", sourceClientEncryptionEnabled);
        props.put("cassandraHostnames", ((Map) ((ArrayList) ((Map) ((ArrayList) map.get("seed_provider")).get(0)).get("parameters")).get(0)).get("seeds"));
        props.put("data_file_directories", map.get("data_file_directories"));
        props.put("native_transport_port", map.get("native_transport_port"));

        Object cqlshrcFilePathObj = env.get("MIGRATE_CQLSHRC_PATH");
        String cqlshrcFilePath = "";
        if (cqlshrcFilePathObj == null)
        {
            cqlshrcFilePath = homeDir + File.separator + ".cassandra" + File.separator + "cqlshrc";
        }
        else
        {
            cqlshrcFilePath = cqlshrcFilePathObj.toString();
        }

    }

    public static void setupCassandraConnection(HashMap props)
    {
        com.datastax.driver.core.Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints(props.get("cassandraHostnames").toString()).withPort(Integer.parseInt((String) props.get("native_transport_port"))).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(100L, 60000L)).withCredentials((String) props.get("cassandraUsername"), (String) props.get("cassandraPassword"));

        boolean hasCassandraSSL = new Boolean((String) props.get("cassandraSSL"));
        SslContextFactory cf = (SslContextFactory) props.get("SslContextFactory");
        if (cf != null && hasCassandraSSL)
        {
            com.datastax.driver.core.JdkSSLOptions jdkSSLOption = com.datastax.driver.core.JdkSSLOptions.builder().withSSLContext(cf.getSslContext()).build();
            builder.withSSL(jdkSSLOption);
        }
        // builder.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("PR1")));
        Cluster cluster = builder.build();

        setSession(cluster.connect());

    }

    public static Session getSession()
    {
        return session;
    }

    public static void setSession(Session session)
    {
        DataMigrator.session = session;
    }

    public DataMigrator(HashMap props)
    {
        setupSSLContextFactor(props);
    }

    public static void setupSSLContextFactor(HashMap props)
    {
        SslContextFactory cf = new SslContextFactory();

        cf.setKeyStorePath((String) props.get("keystorePath"));
        cf.setKeyStorePassword((String) props.get("keystorePassword"));
        cf.setKeyManagerPassword((String) props.get("keystorePassword"));
        cf.setTrustStorePath((String) props.get("truststorePath"));
        cf.setTrustStorePassword((String) props.get("truststorePassword"));
        props.put("SslContextFactory", cf);
    }

    public void startServer(HashMap props) throws Exception
    {
        // Create the Server object and a corresponding ServerConnector and then
        // set the port for the connector. In this example the server will
        // listen on port 8090. If you set this to port 0 then when the server
        // has been started you can called connector.getLocalPort() to
        // programmatically get the port the server started on.
        boolean disableHttp = false;
        if (props.get("FileServerPort") == null)
        {
            disableHttp = true;
        }
        int sslPort = Integer.parseInt((String) props.get("SslFileServerPort"));
        int serverPort = Integer.parseInt((String) props.get("FileServerPort"));
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMaxThreads(50);
        Server server = new Server(threadPool);

        // SSL HTTP Configuration
        // HTTP Configuration
        HttpConfiguration http_config = new HttpConfiguration();
        http_config.setSecureScheme("https");
        http_config.setSecurePort(sslPort);
        http_config.setOutputBufferSize(32768);
        http_config.setRequestHeaderSize(8192);
        http_config.setResponseHeaderSize(8192);
        http_config.setSendServerVersion(true);
        http_config.setSendDateHeader(false);

        if (!disableHttp)
        {
            ServerConnector http = new ServerConnector(server, new HttpConnectionFactory(http_config));
            http.setPort(serverPort);
            http.setIdleTimeout(30000);
            server.addConnector(http);
        }
        HttpConfiguration https_config = new HttpConfiguration(http_config);
        https_config.addCustomizer(new SecureRequestCustomizer());
        SslContextFactory cf = (SslContextFactory) props.get("SslContextFactory");
        ServerConnector sslConnector = new ServerConnector(server, new SslConnectionFactory(cf, HttpVersion.HTTP_1_1.asString()), new HttpConnectionFactory(https_config));
        // SSL Connector

        sslConnector.setPort(sslPort);
        server.addConnector(sslConnector);

        String filePaths = (String) props.get("FileServerPaths");
        String[] filePathArray = filePaths.split(",");
        HashLoginService realm = new HashLoginService("ApplicationRealm");
        realm.setConfig((String) props.get("LoginFilePath"));
        realm.setHotReload(true);

        // A security handler is a jetty handler that secures content behind a
        // particular portion of a url space. The ConstraintSecurityHandler is a
        // more specialized handler that allows matching of urls to different
        // constraints. The server sets this as the first handler in the chain,
        // effectively applying these constraints to all subsequent handlers in
        // the chain.
        ConstraintSecurityHandler security = new ConstraintSecurityHandler();

        // This constraint requires authentication and in addition that an
        // authenticated user be a member of a given set of roles for
        // authorization purposes.

        // This constraint requires authentication and in addition that an
        // authenticated user be a member of a given set of roles for
        // authorization purposes.
        Constraint constraint = new Constraint();
        constraint.setName("auth");
        constraint.setAuthenticate(true);
        constraint.setRoles(new String[] { "user", "admin" });

        // Binds a url pattern with the previously created constraint. The roles
        // for this constraing mapping are mined from the Constraint itself
        // although methods exist to declare and bind roles separately as well.
        ConstraintMapping mapping = new ConstraintMapping();
        mapping.setPathSpec("/*");
        mapping.setConstraint(constraint);

        // First you see the constraint mapping being applied to the handler as
        // a singleton list, however you can passing in as many security
        // constraint mappings as you like so long as they follow the mapping
        // requirements of the servlet api. Next we set a BasicAuthenticator
        // instance which is the object that actually checks the credentials
        // followed by the LoginService which is the store of known users, etc.
        security.setConstraintMappings(Collections.singletonList(mapping));
        security.setAuthenticator(new BasicAuthenticator());
        security.setLoginService(realm);

        ArrayList<Handler> contextHandlers = new ArrayList<Handler>();
        contextHandlers.add(security);
        for (int i = 0; i < filePathArray.length; i++)
        {

            // Create a Context Handler and ResourceHandler. The ContextHandler is
            // getting set to "/" path but this could be anything you like for
            // builing out your url. Note how we are setting the ResourceBase using
            // our jetty maven testing utilities to get the proper resource
            // directory, you needn't use these, you simply need to supply the paths
            // you are looking to serve content from.
            security = new ConstraintSecurityHandler();
            security.setConstraintMappings(Collections.singletonList(mapping));
            security.setAuthenticator(new BasicAuthenticator());
            security.setLoginService(realm);
            ResourceHandler rh0 = new ResourceHandler();

            ContextHandler context0 = new ContextHandler();
            context0.setContextPath("/fs:" + filePathArray[i]);
            File dir0 = new File(filePathArray[i]);
            context0.setBaseResource(org.eclipse.jetty.util.resource.Resource.newResource(dir0));
            context0.setHandler(rh0);
            security.setHandler(context0);
            contextHandlers.add(security);

        }

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        ServletHolder jerseyServlet = context.addServlet(org.glassfish.jersey.servlet.ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);
        // Tells the Jersey Servlet which REST service/class to load.
        jerseyServlet.setInitParameter("jersey.config.server.provider.classnames", PathEntryPoint.class.getCanonicalName());
        security = new ConstraintSecurityHandler();
        security.setConstraintMappings(Collections.singletonList(mapping));
        security.setAuthenticator(new BasicAuthenticator());
        security.setLoginService(realm);
        security.setHandler(context);
        contextHandlers.add(security);

        /**
         * 
         * 
         * ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
         * context.setContextPath("/");
         * 
         * Server jettyServer = new Server(8080); jettyServer.setHandler(context);
         * 
         * ServletHolder jerseyServlet = context.addServlet( org.glassfish.jersey.servlet.ServletContainer.class, "/*");
         * jerseyServlet.setInitOrder(0);
         * 
         * // Tells the Jersey Servlet which REST service/class to load. jerseyServlet.setInitParameter(
         * "jersey.config.server.provider.classnames", EntryPoint.class.getCanonicalName());
         * 
         * try { jettyServer.start(); jettyServer.join(); } finally { jettyServer.destroy(); } }
         * 
         * 
         */

        ContextHandlerCollection contexts = new ContextHandlerCollection();

        // contexts.setHandlers(new Handler[] { context0, context1 });
        contexts.setHandlers(contextHandlers.toArray(new Handler[contextHandlers.size()]));
        server.setHandler(contexts);

        server.addBean(realm);

        server.start();

        // Dump the server state
        System.out.println(server.dump());

        // The use of server.join() the will make the current thread join and
        // wait until the server is done executing.
        // See http://docs.oracle.com/javase/7/docs/api/java/lang/Thread.html#join()
        server.join();
    }
}
