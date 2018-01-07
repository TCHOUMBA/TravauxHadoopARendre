package cs.bigdata.Lab2.graph;


import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class GraphMap extends Mapper<LongWritable, Text, Text, Text> {
	// Overriding of the map method

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		//Get the line 
		String line=value.toString();
		
		if(!line.contains("#")) {
			//Get the edges
			String[] edges=StringUtils.split(line);
			//write the incomming vertice as key
			//Write the outcouming vertice as value
			context.write(new Text(edges[0]), new Text(edges[1]));
		}
	}
}






