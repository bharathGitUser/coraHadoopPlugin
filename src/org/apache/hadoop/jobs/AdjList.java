package org.apache.hadoop.jobs;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.expmanager.utility.ConstantWarehouse;
import org.apache.hadoop.expmanager.utility.Utility;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * This is an example Hadoop Map/Reduce application.
 * It produces list of out-links and in-links for each host.
 * Map reads the text input files each line of the format {A1,A2}, and produces <A1,from{}:to{A2}> and <A2,from{A1}:to{}> tuples
 * as Map output.
 * The Reduce output is a union of all such lists with same key.
 * 
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar adjlist
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */


public class AdjList extends HadoopJobs {

	
	private enum Counter { WORDS, VALUES }
	public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.AdjList");
	public static final int LIMIT = 200000;

	private static int MapTaskTime = 200;
	private static int RedTaskTime = 200;
	
	
	public int getMapTaskTime(){
		return MapTaskTime;
	}
	public int getRedTaskTime(){
		return RedTaskTime;
	}
	
	
	public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, 
				OutputCollector<Text, Text> output, 
				Reporter reporter) throws IOException {

			String line = ((Text)value).toString();
			int index = line.lastIndexOf(",");
			if(index == -1) {
				LOG.info("MAP INPUT IN WRONG FORMAT : " + line);
			}
			String outEdge = line.substring(0,index);
			String inEdge = line.substring(index+1);
			String outList = "from{" + outEdge + "}:to{}";
			String inList = "from{}:to{" + inEdge + "}"; 

			output.collect(new Text(outEdge), new Text(inList));
			reporter.incrCounter(Counter.WORDS, 1);
			output.collect(new Text(inEdge),new Text(outList));
			reporter.incrCounter(Counter.WORDS, 1);
		}
	}

	/**
	 * A reducer class that makes union of all lists.
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, 
				Reporter reporter) throws IOException {

			List<String> fromList = new ArrayList<String>();
			List<String> toList = new ArrayList<String>();
			String str = new String();
			String fromLine = new String();
			String toLine = new String();
			String vertex = new String();
			Text outValue = new Text();
			int r, strLength, index;

			while (values.hasNext()) {
				str = ((Text) values.next()).toString(); 
				strLength = str.length();
				index = str.indexOf(":");
				if(index == -1) {
					LOG.info("REDUCE INPUT IN WRONG FORMAT : " + str);
					continue;
				}
				if(index > 6) // non-empty fromList
					fromLine = str.substring(5,index-1);
				if(index + 5 < strLength) // non-empty toList
					toLine  = str.substring(index+4, strLength-1);

				if(!fromLine.isEmpty()){
					StringTokenizer itr = new StringTokenizer(fromLine,",");
					while(itr.hasMoreTokens()) {
						vertex = new String(itr.nextToken());
						if(!fromList.contains(vertex) && fromList.size() < LIMIT) //avoid heap overflow
							fromList.add(vertex);
					}
				}
				if(!toLine.isEmpty()){
					StringTokenizer itr = new StringTokenizer(toLine,",");
					while(itr.hasMoreTokens()) {
						vertex = new String(itr.nextToken());
						if(!toList.contains(vertex) && toList.size() < LIMIT) // avoid heap overflow
							toList.add(vertex);
					}	
				}
			}
			Collections.sort(fromList);
			Collections.sort(toList);
			String fromList_str = new String("");
			String toList_str = new String("");
			for (r = 0; r < fromList.size(); r++)
				if(fromList_str.equals(""))
					fromList_str = fromList.get(r);
				else
					fromList_str = fromList_str + "," + fromList.get(r);
			for (r = 0; r < toList.size(); r++)
				if(toList_str.equals(""))
					toList_str = toList.get(r);
				else
					toList_str = toList_str + "," + toList.get(r);

			outValue = new Text("from{" + fromList_str + "}:to{" + toList_str + "}");
			output.collect(key,outValue);
			reporter.incrCounter(Counter.WORDS, 1);
		}
	}

	static void printUsage() {
		System.out.println("adjlist [-m <maps>] [-r <reduces>] <input> <output>");
		System.exit(1);
	}

	/**
	 * The main driver for word count map/reduce program.
	 * Invoke this method to submit the map/reduce job.
	 * @throws IOException When there is communication problems with the 
	 *                     job tracker.
	 */










private JobConf initConfig(){
    	
		
    	Path coresitexml = new Path("/usr/local/hadoop/conf/core-site.xml");
		Path mapredsitexml = new Path("/usr/local/hadoop/conf/mapred-site.xml");
		
		Configuration config = new Configuration();
		
		config.addResource(coresitexml);
		config.addResource(mapredsitexml);
		config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
		config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName()   );
		//config.set("fs.default.name", "hdfs://master:54310");
		//config.set("mapred.job.tracker", "master:54311");
	
		
		
		
		JobConf conf = new JobConf(config);
		conf.setJar(ConstantWarehouse.getStringValue("jobJarFilePath"));
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);        
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
	
		
		return conf;
    	
    }




 public RunningJob submitAndExec(String JobName, String QueueID){

		

		JobConf conf = initConfig();

		conf.setJobName(JobName);


		conf.setQueueName(QueueID);



		conf.setNumMapTasks(ConstantWarehouse.getIntegerValue("mapTaskRequest"));
		conf.setNumReduceTasks(ConstantWarehouse.getIntegerValue("redTaskRequest"));
		//conf.setNumMapTasks(100);


		//System.out.println("Queue "+newJob.getQueueId()+" is allocated to job "+newJob.getJobName());

		//String inputPath = Util.pathToInputFilesBase+ newJob.getjobType()+"/";


		String inputPath=ConstantWarehouse.getStringValue("pathToInputFilesBase")+"adj/";
		String outputPath = ConstantWarehouse.getStringValue("pathToOutputFilesBase")+ JobName;
		//Hadoop cannot write output files if they already exist, so clean it up.
		Utility.deleteHdfsDirectory(outputPath);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		try {
			JobClient jc = new JobClient(conf);
			RunningJob rJ = jc.submitJob(conf);
			return rJ;
			//newJob.setRj(rJ);//need this to check later if the job has finished
			//newJob.setLaunchTime(System.currentTimeMillis());


			/*
			Date date=new Date();
			SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			String time=df.format(date);
			System.out.println(newJob.getJobName()+": "+time);
			 */


			// while(rJ.getJobState() != JobStatus.SUCCEEDED);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {


			e.printStackTrace();
		}

		return null;

	}









	public int run(String[] args) throws Exception {

		JobConf conf = initConfig();
		conf.setJobName("adjlist");
		

		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
						args[i-1]);
				printUsage(); // exits
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " +
					other_args.size() + " instead of 2.");
			printUsage();
		}

		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		String outPath = new String(other_args.get(1));
		FileOutputFormat.setOutputPath(conf, new Path(outPath));

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		JobClient.runJob(conf);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took " +
				(end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new AdjList(), args);
		System.exit(ret);
	}
}
