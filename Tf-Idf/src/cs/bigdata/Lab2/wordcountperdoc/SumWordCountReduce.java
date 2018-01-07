package cs.bigdata.Lab2.wordcountperdoc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// To complete according to your problem

public class SumWordCountReduce extends  Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		int wordsPerdoc = 0;
		Map<String, Integer> word_count_map = new HashMap<String, Integer>();
		
		//get The wordsPerdocs
		for (Text value : values) {
			//get the word an his word count
			String[] word_count = value.toString().split("\t");
			//register the word and is word count
			word_count_map.put(word_count[0], Integer.valueOf(word_count[1]));
			//add the word count to the words per docs
			wordsPerdoc += Integer.parseInt(word_count[1]);
		}
		//get the doc name
		String doc=key.toString();
		
		//for each word
		for (String word : word_count_map.keySet()) {
			//get the word count
			int wordCount=word_count_map.get(word);
			
			//The key is ( word docName)
			//write the key with his word count and the words per docs
			context.write(new Text(word + "\t" + doc), new Text(wordCount+ "\t"
					+ wordsPerdoc));
		}
	}



}

