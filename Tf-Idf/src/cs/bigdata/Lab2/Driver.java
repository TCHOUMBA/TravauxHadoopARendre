package cs.bigdata.Lab2;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cs.bigdata.Lab2.tfidf.TfIdfReduce;
import cs.bigdata.Lab2.tfidf.WordWithDocAndWordCountAndWordsPerDocMap;
import cs.bigdata.Lab2.wordcount.SumCountReduce;
import cs.bigdata.Lab2.wordcount.WordInDocWithWordCountMap;
import cs.bigdata.Lab2.wordcountperdoc.DocWithWordCountMap;
import cs.bigdata.Lab2.wordcountperdoc.SumWordCountReduce;


public class Driver extends Configured implements Tool {

	private Job wordCountJOB;
	private Job wordPerDocsJob;
	private Job tfIdfJob;


	public int run(String[] args) throws Exception {

		if (args.length != 2) {

			System.out.println("Usage: [input] [output]");

			System.exit(-1);

		}
		int exitCode=0;

		//Configure and launch the first round: Word count JOB
		Path wordCountInputPath= new Path(args[0]);
		Path wordCountOutputPath= new Path(args[1],"tmp/wordCount");
		configureWordCountJob(wordCountInputPath, wordCountOutputPath);
		if (!wordCountJOB.waitForCompletion(true)) {
			exitCode=1;
			throw new Exception(wordCountJOB.getJobName()+"failed");
		}
		
		//Configure and launch the second round: Words per docs JOB
		Path wordsPerDocOutputPath= new Path(args[1],"tmp/wordsPerDoc");
		configureWordsPerDocJob(wordCountOutputPath, wordsPerDocOutputPath);
		if (!wordPerDocsJob.waitForCompletion(true)) {
			exitCode=1;
			throw new Exception(wordPerDocsJob.getJobName()+"failed");
		}
		
		//Configure and launch the last round: TF IDF JOB
		Path tfIdfOutputPath= new Path(args[1],"out");
		configureTfIdfJob(wordsPerDocOutputPath, tfIdfOutputPath);
		if (!tfIdfJob.waitForCompletion(true)) {
			exitCode=1;
			throw new Exception(tfIdfJob.getJobName()+"failed");
		}

		return exitCode;

	}


	public static void main(String[] args) throws Exception {

		Driver exempleDriver = new Driver();

		int res = ToolRunner.run(exempleDriver, args);

		System.exit(res);

	}

	private void configureWordCountJob(Path inputPath,Path outputPath) throws IllegalArgumentException, IOException {
		// Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

		wordCountJOB = Job.getInstance(getConf());

		wordCountJOB.setJobName("Word Count");


		// On précise les classes MyProgram, Map et Reduce

		wordCountJOB.setJarByClass(Driver.class);

		wordCountJOB.setMapperClass(WordInDocWithWordCountMap.class);

		wordCountJOB.setReducerClass(SumCountReduce.class);


		// Définition des types clé/valeur de notre problème

		wordCountJOB.setMapOutputKeyClass(Text.class);

		wordCountJOB.setMapOutputValueClass(IntWritable.class);


		wordCountJOB.setOutputKeyClass(Text.class);

		wordCountJOB.setOutputValueClass(IntWritable.class);


		// Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)



		FileInputFormat.addInputPath(wordCountJOB, inputPath);

		FileOutputFormat.setOutputPath(wordCountJOB, outputPath);


		//Suppression du fichier de sortie s'il existe déjà

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputPath)) {

			fs.delete(outputPath, true);


		}
	}

	private void configureWordsPerDocJob(Path inputPath,Path outputPath) throws IllegalArgumentException, IOException {
		// Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

		wordPerDocsJob = Job.getInstance(getConf());

		wordPerDocsJob.setJobName("Word per Docs ");


		// On précise les classes MyProgram, Map et Reduce

		wordPerDocsJob.setJarByClass(Driver.class);

		wordPerDocsJob.setMapperClass(DocWithWordCountMap.class);

		wordPerDocsJob.setReducerClass(SumWordCountReduce.class);


		// Définition des types clé/valeur de notre problème

		wordPerDocsJob.setMapOutputKeyClass(Text.class);

		wordPerDocsJob.setMapOutputValueClass(Text.class);


		wordPerDocsJob.setOutputKeyClass(Text.class);

		wordPerDocsJob.setOutputValueClass(Text.class);


		// Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)



		FileInputFormat.addInputPath(wordPerDocsJob, inputPath);

		FileOutputFormat.setOutputPath(wordPerDocsJob, outputPath);


		//Suppression du fichier de sortie s'il existe déjà

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputPath)) {

			fs.delete(outputPath, true);


		}


	}
	private void configureTfIdfJob(Path inputPath,Path outputPath) throws IllegalArgumentException, IOException {
		// Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

		tfIdfJob = Job.getInstance(getConf());

		tfIdfJob.setJobName("TF-IDF  ");


		// On précise les classes MyProgram, Map et Reduce

		tfIdfJob.setJarByClass(Driver.class);

		tfIdfJob.setMapperClass(WordWithDocAndWordCountAndWordsPerDocMap.class);

		tfIdfJob.setReducerClass(TfIdfReduce.class);


		// Définition des types clé/valeur de notre problème

		tfIdfJob.setMapOutputKeyClass(Text.class);

		tfIdfJob.setMapOutputValueClass(Text.class);


		tfIdfJob.setOutputKeyClass(Text.class);

		tfIdfJob.setOutputValueClass(DoubleWritable.class);


		// Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)



		FileInputFormat.addInputPath(tfIdfJob, inputPath);

		FileOutputFormat.setOutputPath(tfIdfJob, outputPath);


		//Suppression du fichier de sortie s'il existe déjà

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputPath)) {

			fs.delete(outputPath, true);


		}


	}

}

