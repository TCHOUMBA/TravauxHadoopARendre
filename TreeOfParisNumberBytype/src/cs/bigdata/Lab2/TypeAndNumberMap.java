package cs.bigdata.Lab2;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TypeAndNumberMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		String lineOfTree=value.toString();
		if (!lineOfTree.contains("HAUTEUR")) {
			
			//Load Tree from the line
			Tree tree =Tree.fromLine(lineOfTree);

			//write the tree Type and his number of Tree
			context.write(new Text(tree.getGenre()+":"+tree.getEspece()+":"+tree.getFamille()), one);	
		}
	}
}






