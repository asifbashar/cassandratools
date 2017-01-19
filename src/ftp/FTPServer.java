package ftp;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;

public class FTPServer {
    
    public static void main(String[] args) throws FtpException {
        
    }
    
    public void startFtp() throws FtpException
    {
    	PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        UserManager userManager = userManagerFactory.createUserManager();
        BaseUser user = new BaseUser();
        user.setName("username");
        user.setPassword("password");
        user.setHomeDirectory("/tmp");
        userManager.save(user);
        
        ListenerFactory listenerFactory = new ListenerFactory();
        listenerFactory.setPort(2221);
        
        FtpServerFactory factory = new FtpServerFactory();
        factory.setUserManager(userManager);
        factory.addListener("default", listenerFactory.createListener());
        
        FtpServer server = factory.createServer();
        server.start();
    }
}