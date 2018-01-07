package cs.bigdata.Lab2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cs.bigdata.Lab2.graph.GraphMap;
import cs.bigdata.Lab2.graph.GraphReduce;
import cs.bigdata.Lab2.pageranking.PageRankMap;
import cs.bigdata.Lab2.pageranking.PageRankReduce;
import cs.bigdata.Lab2.pageranking.damping.DampingPageRankMap;


public class Driver extends Configured implements Tool {

	private Job graphJob;
	private Job pageRankJob;
	private Job dampingPageRankJob;


	public int run(String[] args) throws Exception {

		if (args.length != 3) {

			System.out.println("Usage: [input] [output] [max iterations]");

			System.exit(-1);

		}

		int exitCode=0;
		Path graphInputPath=new Path(args[0]);
		Path graphOutputPath=new Path(args[1],"graph");
		//configure and launch the first round : the graph job
		configureGraphJob(graphInputPath, graphOutputPath);
		if (!graphJob.waitForCompletion(true)) {
			exitCode=1;
			throw new Exception(graphJob.getJobName()+"failed");
		}

		
		
		final int MAX_ITERATION=Integer.valueOf(args[2]);
		int iteration=1;
		Path pageRankInputPath=graphOutputPath;
		Path pageRankOutputPath;
		Path dampingPageRankOutputPath;
	

		while (iteration<=MAX_ITERATION) {
			pageRankOutputPath= new Path(args[1],"tmp/pageRank-"+iteration);
			dampingPageRankOutputPath= new Path(args[1],"out/pageRank-"+iteration);

			//configure and launch the second round : the page rank job
			configurePageRankJob(pageRankInputPath,pageRankOutputPath,iteration);
			if (!pageRankJob.waitForCompletion(true)) {
				exitCode=1;
				throw new Exception(pageRankJob.getJobName()+"failed");
			}

			//configure and launch the last round : the damping page rank job
			configureDampingPageRankJob(pageRankOutputPath,dampingPageRankOutputPath,iteration);
			if (!dampingPageRankJob.waitForCompletion(true)) {
				exitCode=1;
				throw new Exception(dampingPageRankJob.getJobName()+"failed");
			}

//			final  double CONVERGENCE=0.01;
//			final  int SCALED_FACTOR=1000;
//
//		//get the scaled sum of pageRank diff
//			long scaledSumOfPageRankDiff=dampingPageRankJob.getCounters().findCounter(Counter.PAGE_RANK_DIFF).getValue();
//
//			//Get the current convergence
//			Long totalNumberOfNodes=getConf().getLong("TOTAL_NUMBER_OF_NODES", 0L);
//			double currentConvergence=(double)scaledSumOfPageRankDiff/((double)SCALED_FACTOR*totalNumberOfNodes);
//            System.out.println(String.format("Current convergence : %s", currentConvergence));
//			
//            //check the current convergence
//			if (currentConvergence<=CONVERGENCE) {
//				break;
//			}

			pageRankInputPath= dampingPageRankOutputPath;
			iteration++;

		}

		return exitCode;
	}


	public static void main(String[] args) throws Exception {

		Driver exempleDriver = new Driver();

		int res = ToolRunner.run(exempleDriver, args);

		System.exit(res);

	}

	private  void configureGraphJob(Path inputPath,Path outputPath) throws IOException {
		// Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

		Configuration conf= getConf();
		conf.setLong("TOTAL_NUMBER_OF_NODES", 75879L);

		graphJob = Job.getInstance(conf);

		graphJob.setJobName("Make Graph file");


		// On précise les classes MyProgram, Map et Reduce

		graphJob.setJarByClass(Driver.class);

		graphJob.setMapperClass(GraphMap.class);

		graphJob.setReducerClass(GraphReduce.class);


		// Définition des types clé/valeur de notre problème

		graphJob.setMapOutputKeyClass(Text.class);

		graphJob.setMapOutputValueClass(Text.class);


		graphJob.setOutputKeyClass(Text.class);

		graphJob.setOutputValueClass(Text.class);


		// Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)

		Path outputFilePath= outputPath;

		FileInputFormat.addInputPath(graphJob, inputPath);

		FileOutputFormat.setOutputPath(graphJob, outputFilePath);


		//Suppression du fichier de sortie s'il existe déjà

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputFilePath)) {

			fs.delete(outputFilePath, true);

		}


	}
	private  void configurePageRankJob(Path inputPath,Path outputPath,int iteration) throws IOException {
		// Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

		Configuration conf= getConf();

		pageRankJob = Job.getInstance(conf);

		pageRankJob.setJobName("Process Page Rank");


		// On précise les classes MyProgram, Map et Reduce

		pageRankJob.setJarByClass(Driver.class);

		pageRankJob.setMapperClass(PageRankMap.class);

		pageRankJob.setReducerClass(PageRankReduce.class);


		// Définition des types clé/valeur de notre problème

		pageRankJob.setMapOutputKeyClass(Text.class);

		pageRankJob.setMapOutputValueClass(Text.class);


		pageRankJob.setOutputKeyClass(Text.class);

		pageRankJob.setOutputValueClass(Text.class);


		// Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)

		FileInputFormat.addInputPath(pageRankJob, inputPath);

		FileOutputFormat.setOutputPath(pageRankJob, outputPath);


		//Suppression du fichier de sortie s'il existe déjà

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputPath)) {

			fs.delete(outputPath, true);

		}


	}
	private  void configureDampingPageRankJob(Path inputPath,Path outputPath,int iteration) throws IOException {
		// Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

		long totalDanglingNodes=pageRankJob.getCounters().findCounter(Counter.DANGLING_NODES).getValue();
		getConf().setLong("TOTAL_NUMBER_OF_DANGLING_NODES", totalDanglingNodes);
		getConf().setDouble("DAMPING_FACTOR", 0.85);
		Configuration conf= getConf();

		dampingPageRankJob = Job.getInstance(conf);

		dampingPageRankJob.setJobName("Process Page Rank with Damping");


		// On précise les classes MyProgram, Map et Reduce

		dampingPageRankJob.setJarByClass(Driver.class);

		dampingPageRankJob.setMapperClass(DampingPageRankMap.class);


		// Définition des types clé/valeur de notre problème

		dampingPageRankJob.setMapOutputKeyClass(Text.class);

		dampingPageRankJob.setMapOutputValueClass(Text.class);


		// Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)

		FileInputFormat.addInputPath(dampingPageRankJob, inputPath);

		FileOutputFormat.setOutputPath(dampingPageRankJob, outputPath);


		//Suppression du fichier de sortie s'il existe déjà

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputPath)) {

			fs.delete(outputPath, true);

		}


	}

}

