package org.apache.hadoop.jobs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.expmanager.utility.ConstantWarehouse;
import org.apache.hadoop.expmanager.utility.Utility;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Classification extends HadoopJobs {


	
	private enum Counter { WORDS, VALUES }
	
private static int clusterNum=6;
	private static String strModelFile = "/home/hduser/inCen";
	public static Cluster[] centroids = new Cluster[clusterNum];
	public static Cluster[] centroids_ref = new Cluster[clusterNum];


	public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.classification");

	//public static String Util.strModelFile = "/localhome/hadoop1/work/kmeans/mr_centroids";
	


	// Input data should have the following format. Each line of input record represents one movie and all of its reviews. 
	// Each record has the format: <movie_id><:><reviewer><_><rating><,><movie_id><:><reviewer><_><rating><,> .....

	final private int MapTaskTime = 30;
	final private int RedTaskTime = 120;
	
	
	public int getMapTaskTime(){
		return MapTaskTime;
	}
	public int getRedTaskTime(){
		return RedTaskTime;
	}
	
	
	public static class MapClass extends MapReduceBase implements
	Mapper<LongWritable, Text, IntWritable, Text> {
		private int totalClusters;


		public void configure(JobConf conf){
			try {
				totalClusters = initializeCentroids();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
						throws IOException {
			String movieIdStr = new String();
			String reviewStr = new String();
			String userIdStr = new String();
			String reviews = new String();
			String line = new String();
			String tok = new String("");
			long movieId;
			int review, userId, p,q,r,rater,rating,movieIndex;
			int clusterId = 0;
			int[] n = new int[clusterNum];
			float[] sq_a = new float[clusterNum];
			float[] sq_b = new float[clusterNum];
			float[] numer = new float[clusterNum];
			float[] denom = new float[clusterNum];
			float max_similarity = 0.0f;
			float similarity = 0.0f;
			Cluster movie = new Cluster();

			line = ((Text) value).toString();
			movieIndex = line.indexOf(":");
			for (r = 0; r <clusterNum; r++) {
				numer[r] = 0.0f;
				denom[r] = 0.0f;
				sq_a[r] = 0.0f;
				sq_b[r] = 0.0f;
				n[r] = 0;
			}
			if (movieIndex > 0) {
				movieIdStr = line.substring(0, movieIndex);
				movieId = Long.parseLong(movieIdStr);
				movie.movie_id = movieId;
				reviews = line.substring(movieIndex + 1);
				StringTokenizer token = new StringTokenizer(reviews, ",");

				while (token.hasMoreTokens()) {
					tok = token.nextToken();
					int reviewIndex = tok.indexOf("_");
					if(reviewIndex>=0){
						userIdStr = tok.substring(0, reviewIndex);
						reviewStr = tok.substring(reviewIndex + 1);
						userId = Integer.parseInt(userIdStr);
						review = Integer.parseInt(reviewStr);
						for (r = 0; r < totalClusters; r++) {
							for (q = 0; q < centroids_ref[r].total; q++) {
								rater = centroids_ref[r].reviews.get(q).rater_id;
								rating = (int) centroids_ref[r].reviews.get(q).rating;
								if (userId == rater) {
									numer[r] += (float) (review * rating);
									sq_a[r] += (float) (review * review);
									sq_b[r] += (float) (rating * rating);
									n[r]++; // counter 
									break; // to avoid multiple ratings by the same reviewer
								}
							}
						}
					}
				}
				for (p = 0; p < totalClusters; p++) {
					denom[p] = (float) ((Math.sqrt((double) sq_a[p])) * (Math
							.sqrt((double) sq_b[p])));
					if (denom[p] > 0) {
						similarity = numer[p] / denom[p];
						if (similarity > max_similarity) {
							max_similarity = similarity;
							clusterId = p;
						}
					}
				}
				output.collect(new IntWritable(clusterId), new Text(movieIdStr));
				reporter.incrCounter(Counter.WORDS, 1);
			}
		}
		public void close() {
		}
	}


	public static class Reduce extends MapReduceBase implements
	Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
						throws IOException {

			while (values.hasNext()) {
				Text cr = (Text) values.next();
				output.collect(key, cr);
				reporter.incrCounter(Counter.VALUES, 1);
			}
		}
	}

	static void printUsage() {
		System.out
		.println("classification [-m <maps>] [-r <reduces>] <input> <output>");
		System.exit(1);
	}

	public static int initializeCentroids() throws FileNotFoundException {
		int i, k, index, numClust = 0;
		Review rv;
		String reviews = new String("");
		String SingleRv = new String("");
		File modelFile;
		Scanner opnScanner;
		for (i = 0; i < clusterNum; i++) {
			centroids[i] = new Cluster();
			centroids_ref[i] = new Cluster();
		}
		modelFile = new File(strModelFile);
		opnScanner = new Scanner(modelFile);
		while(opnScanner.hasNext()){
			k = opnScanner.nextInt();
			centroids_ref[k].similarity = opnScanner.nextFloat();
			centroids_ref[k].movie_id = opnScanner.nextLong();
			centroids_ref[k].total = opnScanner.nextShort();
			reviews = opnScanner.next();
			Scanner revScanner = new Scanner(reviews);
			revScanner.useDelimiter(",");
			while(revScanner.hasNext()){
				SingleRv = revScanner.next();
				index = SingleRv.indexOf("_");
				String reviewer = new String(SingleRv.substring(0,index));
				String rating = new String(SingleRv.substring(index+1));
				rv = new Review();
				rv.rater_id = Integer.parseInt(reviewer);
				rv.rating = (byte) Integer.parseInt(rating);
				centroids_ref[k].reviews.add(rv);
			}
			revScanner.close();
		}
		opnScanner.close();
		// implementing naive bubble sort as ConstantWarehouse.getConstantValue("maxClusters") is small
		// sorting is done to assign top most cluster ids in each iteration
		for( int pass = 1; pass < clusterNum; pass++) {
			for (int u = 0; u < clusterNum - pass; u++) {
				if (centroids_ref[u].movie_id < centroids_ref[u+1].movie_id ) {
					Cluster temp = new Cluster(centroids_ref[u]);
					centroids_ref[u] = centroids_ref[u+1];
					centroids_ref[u+1] = temp;
				}
			}
		}
		for(int l=0; l< clusterNum; l++) {
			if(centroids_ref[l].movie_id != -1){
				numClust++;
			}
		}
		return numClust;
	}











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
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);
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

		String inputPath=ConstantWarehouse.getStringValue("pathToInputFilesBase")+"movie/";

		String outputPath =ConstantWarehouse.getStringValue("pathToOutputFilesBase")+ JobName;
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
		int i;
		String outPath;
		int numMaps = 0, numReds = 0;

		List<String> other_args = new ArrayList<String>();
		for (i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					numMaps = Integer.parseInt(args[++i]);
				} else if ("-r".equals(args[i])) {
					numReds = Integer.parseInt(args[++i]);
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of "
						+ args[i]);
				printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "
						+ args[i - 1]);
				printUsage(); // exits
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 2.");
			printUsage();
		}

		Date startTime = new Date();
		LOG.info("Job started: " + startTime);
		Date startIteration;
		Date endIteration;
		JobConf conf = initConfig();
		conf.setJobName("classification");
		
		conf.setNumMapTasks(numMaps);
		conf.setNumReduceTasks(numReds);
		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		outPath = new String(other_args.get(1));
		FileOutputFormat.setOutputPath(conf, new Path(outPath));
		startIteration = new Date();
		JobClient.runJob(conf);
		endIteration = new Date();


		LOG.info("The iteration took "  + (endIteration.getTime() - startIteration.getTime()) / 1000
				+ " seconds.");
		return 0;
	}
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Classification(), args);
		System.exit(ret);
	}









}
