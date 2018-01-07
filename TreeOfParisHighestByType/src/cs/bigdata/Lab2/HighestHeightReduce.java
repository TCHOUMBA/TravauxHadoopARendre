package cs.bigdata.Lab2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// To complete according to your problem

public class HighestHeightReduce extends  Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double highestHeight = 0;
		
		//get the highest height of the Key
		for (DoubleWritable val : values) {
			highestHeight=Math.max(highestHeight, val.get());
		}
		//write the key and his highest height
		context.write(key, new DoubleWritable(highestHeight));
	}



}

