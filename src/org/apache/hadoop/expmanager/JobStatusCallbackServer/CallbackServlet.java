package org.apache.hadoop.expmanager.JobStatusCallbackServer;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



public class CallbackServlet  extends HttpServlet{
	private static final long serialVersionUID = 5L;

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{


		PrintWriter out = response.getWriter();

		response.setContentType("text/html");
		response.setStatus(HttpServletResponse.SC_OK);
		out.println("<h1>Update received.\n");

		
		
		
		String jobID=request.getParameter("jobID");
		String status=request.getParameter("status");
		System.out.println(jobID + " " + status);   
		
		if((!jobID.equalsIgnoreCase("null"))&&(!status.equalsIgnoreCase("null"))){
			//TODO: process when job is done
			
		}
		
		

	}	 




}



