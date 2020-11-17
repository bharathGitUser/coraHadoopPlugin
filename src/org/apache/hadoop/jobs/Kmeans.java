package org.apache.hadoop.jobs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.jobs.ClusterWritable;


public class Kmeans extends HadoopJobs{


	//  private static final Log LOG = LogFactory.getLog(Kmeans.class);
	private enum Counter {
		WORDS, VALUES
	}

	
private static int clusterNum=6;
	private static String strModelFile = "/home/hduser/inCen";
	
	public static Cluster[] centroids = new Cluster[clusterNum];
	public static Cluster[] centroids_ref = new Cluster[clusterNum];

	public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.classification");

	//public static String Util.strModelFile = "/localhome/hadoop1/work/kmeans/mr_centroids";
	
	// Input data should have the following format. Each line of input record represents one movie and all of its reviews. 
	// Each record has the format: <movie_id><:><reviewer><_><rating><,><movie_id><:><reviewer><_><rating><,> .....

	/* The kmeans algorithm works as follows: The input data contains lists of movies with reviews by different reviewers.
	 * We need to make k clusters of similar movies. The similarity among movies is determined by the ratings given to
	 * two movies by the same reviewer. 
	 * Initialization: start with k centroids, each centroid represents a movie alongwith the reviews. The selection of value 
	 * k and the centroids for each cluster can be determined using some clustering initialization algorithms such as canopy clustering.
	 * We assume that the value of k and k centroids are pre-determined and provided to the program through local or global file system.
	 * 
	 * Map phase: Scan through input, measure similarity of all movies with the centroids, and emit <closest_centroid,<similarity, movies_data>>
	 * where movies_data is same as the input record. This movies_data needs to be propagated so that when reduce function selects a new
	 * centroid for the next iteration, it can attach the corresponding reviews with the centroid. 
	 * 
	 * Reduce phase: Collect data from map phase pertaining to one centroid and compute a new centroid by averaging the similarity values for all
	 * movies in the cluster. The new centroid is selected as a movie whose similarity is closest to the average similarity value of the cluster.
	 * 
	 */

	final private int MapTaskTime = 20;
	final private int RedTaskTime = 20;
	
	
	public int getMapTaskTime(){
		return MapTaskTime;
	}
	public int getRedTaskTime(){
		return RedTaskTime;
	}
	

	public static class MapClass extends MapReduceBase implements
	Mapper<LongWritable, Text, IntWritable, ClusterWritable> {
		private int totalClusters;


		public void configure(JobConf conf){
			try {
				totalClusters = initializeCentroids();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, ClusterWritable> output, Reporter reporter)
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
			ClusterWritable movies_arrl = new ClusterWritable();

			line = ((Text) value).toString();
			movieIndex = line.indexOf(":");
			for (r = 0; r < clusterNum; r++) {
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

				movies_arrl.movies.add(line);
				movies_arrl.similarities.add(max_similarity);
				movies_arrl.similarity = max_similarity;
				output.collect(new IntWritable(clusterId), movies_arrl);
				reporter.incrCounter(Counter.WORDS, 1);
			}
		}
		public void close() {
		}
	}


	public static class Reduce extends MapReduceBase implements
	Reducer<IntWritable, ClusterWritable, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterator<ClusterWritable> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
						throws IOException {

			float sumSimilarity = 0.0f;
			int numMovies = 0;
			float avgSimilarity = 0.0f;
			float similarity = 0.0f;
			int s = 0;
			int count;
			float diff = 0.0f;
			float minDiff = 1.0f;
			int candidate = 0;
			String data = new String("");
			String shortline = new String("");
			ArrayList<String> arrl = new ArrayList<String>();
			ArrayList<Float> simArrl = new ArrayList<Float>();
			String oneElm = new String();
			int indexShort, index2;
			Text val = new Text();

			while (values.hasNext()) {
				ClusterWritable cr = (ClusterWritable) values.next();
				similarity = cr.similarity;
				simArrl.addAll(cr.similarities);
				for (int i = 0; i < cr.movies.size();i++) {
					oneElm = cr.movies.get(i);
					indexShort = oneElm.indexOf(",",1000); // to avoid memory error caused by long arrays; it will results less accurate
					if(indexShort == -1){
						shortline = new String(oneElm);
					}
					else {
						shortline = new String(oneElm.substring(0, indexShort));
					}
					arrl.add(shortline);
					output.collect(key, new Text(oneElm));
				}
				numMovies += cr.movies.size();
				sumSimilarity += similarity;
			}
			if (numMovies > 0){
				avgSimilarity = sumSimilarity / (float) numMovies;
			}
			diff = 0.0f;
			minDiff = 1.0f;
			for (s = 0; s < numMovies; s++) {
				diff = (float) Math.abs(avgSimilarity - simArrl.get(s));
				if (diff < minDiff) {
					minDiff = diff;
					candidate = s;
				}
			}
			data = arrl.get(candidate);
			index2 = data.indexOf(":");
			String movieStr = data.substring(0, index2);
			String reviews = data.substring(index2+1);
			StringTokenizer token = new StringTokenizer(reviews, ",");
			count = 0;
			while (token.hasMoreTokens()) {
				token.nextToken();
				count++;
			}
			LOG.info("The key = " + key.toString() + " has members = " + numMovies + " simil = " + simArrl.get(candidate));
			val = new Text(simArrl.get(candidate) + " " + movieStr + " " + count + " " + reviews);
			output.collect(key, val);
			reporter.incrCounter(Counter.VALUES, 1);

		}
	}

	static void printUsage() {
		System.out
		.println("kmeans [-m <maps>] [-r <reduces>] <input> <output>");
		System.exit(1);
	}

	public static int initializeCentroids() throws FileNotFoundException {
		int i, k, index, numClust = 0;
		Review rv;
		String reviews = new String();
		String singleRv = new String();
		String reviewer = new String();
		String rating = new String();
		for (i = 0; i < clusterNum; i++) {
			centroids[i] = new Cluster();
			centroids_ref[i] = new Cluster();
		}
		File modelFile = new File(strModelFile);
		Scanner opnScanner = new Scanner(modelFile);
		while(opnScanner.hasNext()){
			k = opnScanner.nextInt();
			centroids_ref[k].similarity = opnScanner.nextFloat();
			centroids_ref[k].movie_id = opnScanner.nextLong();
			centroids_ref[k].total = opnScanner.nextShort();
			reviews = opnScanner.next();
			Scanner revScanner = new Scanner(reviews);
			revScanner.useDelimiter(",");
			while(revScanner.hasNext()){
				singleRv = revScanner.next();
				index = singleRv.indexOf("_");
				reviewer = new String(singleRv.substring(0,index));
				rating = new String(singleRv.substring(index+1));
				rv = new Review();
				rv.rater_id = Integer.parseInt(reviewer);
				rv.rating = (byte) Integer.parseInt(rating);
				centroids_ref[k].reviews.add(rv);
			}
			revScanner.close();
		}
		opnScanner.close();
		// implementing naive bubble sort as Util.maxClusters is small
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
		conf.setJarByClass(Kmeans.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(ClusterWritable.class);
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





	@Override
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
		conf.setJobName("kmeans");
		
		conf.setNumMapTasks(numMaps);
		conf.setNumReduceTasks(numReds);
		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		outPath = new String(other_args.get(1));
		FileOutputFormat.setOutputPath(conf, new Path(outPath));
		startIteration = new Date();
		JobClient.runJob(conf);
		endIteration = new Date();
		LOG.info("The iteration took "
				+ (endIteration.getTime() - startIteration.getTime()) / 1000
				+ " seconds.");
		return 0;
	}


	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new InvertedIndex(), args);
		System.exit(ret);
	}

}
