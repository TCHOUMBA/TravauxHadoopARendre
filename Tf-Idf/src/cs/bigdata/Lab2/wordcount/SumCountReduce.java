package cs.bigdata.Lab2.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SumCountReduce extends  Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int wordCount = 0;
		
		//get the total word Count
		for (IntWritable val : values) {
			wordCount += val.get();
		}
		//write the key and his  word Count
		context.write(key, new IntWritable(wordCount));
	}



}

