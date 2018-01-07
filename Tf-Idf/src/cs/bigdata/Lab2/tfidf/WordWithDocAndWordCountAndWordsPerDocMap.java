package cs.bigdata.Lab2.tfidf;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

// To complete according to your problem
public class WordWithDocAndWordCountAndWordsPerDocMap extends Mapper<LongWritable, Text, Text, Text> {
	// Overriding of the map method

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		//get the tuples word doc and word count in the doc
		String[] word_doc_count_wordsPerDoc = value.toString().split("\t");

		//get the word
		String word=word_doc_count_wordsPerDoc[0];
		//get the docName
		String doc=word_doc_count_wordsPerDoc[1];
		//get the  word count
		String wordCount=word_doc_count_wordsPerDoc[2];
		//get the words Per Docs
		String wordsPerDoc=word_doc_count_wordsPerDoc[3];

		//the key is the word
		//write the key with is doc and word count and words per doc
		context.write(new Text(word), new Text(doc + "\t" + wordCount +"\t"+wordsPerDoc));
	}
}






