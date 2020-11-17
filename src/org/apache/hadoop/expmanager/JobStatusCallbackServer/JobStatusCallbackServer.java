package org.apache.hadoop.expmanager.JobStatusCallbackServer;



import java.util.Random;

import org.apache.hadoop.expmanager.utility.ConstantWarehouse;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.servlet.ServletHandler;

public class JobStatusCallbackServer
{
   
   


   public static Server startServer()throws Exception {
    	Server server = new Server();
    	Random rand = new Random();
    	int port = rand.nextInt(500 + 1) + 60000;

    	

    	Connector connector=new SocketConnector();
    	connector.setPort(port);
    	connector.setHost(ConstantWarehouse.getStringValue("JobStatusCallbackAddress"));
    	server.setConnectors(new Connector[]{connector});

        ServletHandler handler=new ServletHandler();
        server.setHandler(handler);
        
        handler.addServletWithMapping("org.apache.hadoop.expmanager.JobStatusCallbackServer.CallbackServlet", "/");

        server.start();
        server.join();
        
       
        
        return server;
	}

}



















