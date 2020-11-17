package org.apache.hadoop.expmanager.utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.hadoop.expmanager.HadoopJobProxy;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Utility {

	
public static void switchLogFile(String loggerName, String newLogFile){
		
		
		Logger LoggerTemp= Logger.getLogger(loggerName);
		LoggerTemp.removeAllAppenders();
		
		PatternLayout layoutTemp = new PatternLayout();  
		layoutTemp.setConversionPattern("[%-5p] %d - %m%n");
		
		
		try {
			FileAppender appender = new FileAppender(layoutTemp,newLogFile,false);
			LoggerTemp.addAppender(appender);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//LoggerTemp.addAppender(appender);
		
		
	}
	
	
	
public static void copyFiles(String destinationFolder){



	try {
		//this executes the command like it would be executed on the linux terminal
		String line;

		Process p = Runtime.getRuntime().exec("mkdir -p /home/hduser/DataUseful/"+destinationFolder);
		p.waitFor();

		BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		while((line = error.readLine()) != null){


			System.out.println(line);


		}
		error.close();





		p = Runtime.getRuntime().exec("cp -r /home/hduser/log /home/hduser/DataUseful/"+destinationFolder+"/");
		p.waitFor();



		error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		while((line = error.readLine()) != null){


			System.out.println(line);


		}
		error.close();




		p = Runtime.getRuntime().exec("mkdir /home/hduser/log");
		p.waitFor();



		error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		while((line = error.readLine()) != null){


			System.out.println(line);


		}
		error.close();




		p = Runtime.getRuntime().exec("cp /home/hduser/DataUseful/"+destinationFolder+"/log/jobSet.txt /home/hduser/log/");
		p.waitFor();



		error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		while((line = error.readLine()) != null){


			System.out.println(line);


		}
		error.close();

		/*
		BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
		while((line=input.readLine()) != null){
			System.out.println(line);
		}

		input.close();
		 */



	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}


}


	public static void replaceJobSetFile(int iteration){

		String line;
		Process p;
		try {

			p = Runtime.getRuntime().exec("cp /home/hduser/DataUseful/"+iteration+".txt /home/hduser/log/jobSet.txt");

			p.waitFor();
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while((line = input.readLine()) != null){


				System.out.println(line);


			}
			input.close();

			BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while((line = error.readLine()) != null){


				System.out.println(line);


			}

			error.close();





		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	
	
	
	
	public static void restartCluster(){

		String line;
		Process p;
		try {

			p = Runtime.getRuntime().exec("/home/hduser/utility/stopall.sh");

			p.waitFor();
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while((line = input.readLine()) != null){


				System.out.println(line);


			}
			input.close();

			BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while((line = error.readLine()) != null){


				System.out.println(line);


			}

			error.close();







			p = Runtime.getRuntime().exec("/home/hduser/utility/startall.sh");

			p.waitFor();
			input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while((line = input.readLine()) != null){


				System.out.println(line);


			}
			input.close();

			error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while((line = error.readLine()) != null){


				System.out.println(line);


			}

			error.close();






		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
	}
	
	
	
	
	
	
	
	public static void deleteHdfsDirectory(String dir){
		try {
			String command = "/usr/local/hadoop/bin/hdfs dfs -rm -r "+ dir;
			//this executes the command like it would be executed on the linux terminal
			Process p = Runtime.getRuntime().exec(command);
			p.waitFor();
			
			/*
			String line;

			BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while((line = error.readLine()) != null){
				
				System.out.println(line);
			}
			error.close();
			*/
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void clearContentsOfFile(String fileName){
		try {
			PrintWriter writer = new PrintWriter(fileName);
			writer.print("");
			writer.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}
	
	public static void StoreResult(ArrayList<HadoopJobProxy> setOfJobs){



		try {
			// Prepare to read from the file, using a Scanner object
			File file = new File(ConstantWarehouse.getStringValue("jobResultFile"));

			FileWriter fw;

			fw = new FileWriter(file.getAbsoluteFile());

			BufferedWriter bw = new BufferedWriter(fw);

			for (int i=0; i<setOfJobs.size(); i++){

				String line=setOfJobs.get(i).getTotalExecutionTime()+  "	"+ setOfJobs.get(i).getLatency() 
						+ "	"+setOfJobs.get(i).calcUtility(setOfJobs.get(i).getTotalExecutionTime())
						+"	"+setOfJobs.get(i).getmapTaskRequested()+"	"+setOfJobs.get(i).getredTaskRequested()
						+"	"+ setOfJobs.get(i).getJobDeadlinewhenSubmit() + "	"+ setOfJobs.get(i).getTimetoJoin()
						+"	"+setOfJobs.get(i).getPriority()+"	"+setOfJobs.get(i).getjobDeadlineType()
						+"	"+ setOfJobs.get(i).getjobClass().getSimpleName()+"\r\n";

				bw.write(line);
			}

			bw.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



	}
	
	
	
	
	
	
	
}
