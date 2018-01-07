package cs.bigdata.Lab2.wordcountperdoc;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DocWithWordCountMap extends Mapper<LongWritable, Text, Text, Text> {
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		//get the tuples word doc and word count in the doc
		String[] word_doc_count = value.toString().split("\t");
		
		//get the word
		String word=word_doc_count[0];
		//get the docName
		String doc=word_doc_count[1];
		//get the the word count
		String count=word_doc_count[2];
		
		//the key is the doc name
		//write the key with is word and word count
		context.write(new Text(doc), new Text(word + "\t" + count));


	}
}






