package cs.bigdata.Lab2;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.base.Strings;

import cs.bigdata.Lab2.Tree;

// To complete according to your problem
public class TypeAndHeightMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		String lineOfTree=value.toString();
		if (!lineOfTree.contains("HAUTEUR")) {
			
			//Load Tree from the line
			Tree tree =Tree.fromLine(lineOfTree);

			//Set empty height to 0;
			Double height=Strings.isNullOrEmpty(tree.getHauteur())?0d:Double.valueOf(tree.getHauteur());
			
			//write the tree Type and his height
			context.write(new Text(tree.getGenre()+":"+tree.getEspece()+":"+tree.getFamille()), new DoubleWritable(height));	

		}

	}
}






