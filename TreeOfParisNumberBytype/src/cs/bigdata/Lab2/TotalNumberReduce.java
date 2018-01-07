package cs.bigdata.Lab2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// To complete according to your problem

public class TotalNumberReduce extends  Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int totalNumberOfTree = 0;
		
		//Sum all number of Tree of the Key
		for (IntWritable val : values) {
			totalNumberOfTree += val.get();
		}
		//write The Tree Type with his total total number of Tree
		context.write(key, new IntWritable(totalNumberOfTree));
	}



}

