

package org.apache.hadoop.jobs;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.expmanager.utility.ConstantWarehouse;
import org.apache.hadoop.expmanager.utility.Utility;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
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


public class WordCount extends HadoopJobs{

	final private int MapTaskTime = 40;//145;
	final private int RedTaskTime = 80;//170;
	
	
	public int getMapTaskTime(){
		return MapTaskTime;
	}
	public int getRedTaskTime(){
		return RedTaskTime;
	}
	

	public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,	Reporter reporter) throws IOException {

			Text word = new Text();

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}


		}
	}

	/**
	 * A reducer class that makes union of all lists.
	 */
	public static class ReduceClass extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {


		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
						throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
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
		
		
	//	.setJarByClass(WordCount.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapClass.class);        
		conf.setCombinerClass(ReduceClass.class);
		conf.setReducerClass(ReduceClass.class);
		
		
		
		
		return conf;
    	
    }





	public RunningJob submitAndExec(String JobName, String QueueID){
		

		JobConf conf =initConfig();
		conf.setJobName(JobName);
		conf.setQueueName(QueueID);
		
		
		conf.setNumMapTasks(ConstantWarehouse.getIntegerValue("mapTaskRequest"));
		conf.setNumReduceTasks(ConstantWarehouse.getIntegerValue("redTaskRequest"));
		
		//String inputPath=ConstantWarehouse.getStringValue("pathToInputFilesBase")+"smallData/wiki/";

		String inputPath=ConstantWarehouse.getStringValue("pathToInputFilesBase")+"wiki/";
		String outputPath = ConstantWarehouse.getStringValue("pathToOutputFilesBase")+ JobName;
		//Hadoop cannot write output files if they already exist, so clean it up.
		Utility.deleteHdfsDirectory(outputPath);
		
		//conf.setJar("/home/hduser/jobManager.jar");
		
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


	
	static void printUsage() {
		System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
		System.exit(1);
	}

	

	public int run(String[] args) throws Exception {

		

		
		JobConf conf =initConfig();
		conf.setJobName("wordcount");
		
		


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
		int ret = ToolRunner.run(new WordCount(), args);
		System.exit(ret);
	}







}

