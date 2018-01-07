package cs.bigdata.Lab2.wordcount;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordInDocWithWordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable COUNT= new IntWritable(1);
	private final static String SEARCH_REGEX="\\w+";

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		//make the search  Regex
		Pattern p = Pattern.compile(SEARCH_REGEX);
		Matcher m = p.matcher(value.toString());
		
		//Get the doc Name
		String docName = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		while(m.find()) {
			//find a word
			String word=m.group();
			
			//the Key is (word \t docName)
			//write the key and the count
			context.write(new Text(word+"\t"+docName),COUNT);
		}
		

	}
}






