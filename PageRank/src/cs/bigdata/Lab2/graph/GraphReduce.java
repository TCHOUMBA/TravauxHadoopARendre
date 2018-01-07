package cs.bigdata.Lab2.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs.bigdata.Lab2.Vertice;


// To complete according to your problem

public class GraphReduce extends  Reducer<Text, Text, Text, Text> {
	private double TOTAL_NUMBER_OF_NODES=0L;
	
	

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		TOTAL_NUMBER_OF_NODES=context.getConfiguration().getLong("TOTAL_NUMBER_OF_NODES", 1L);
	}



	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
	    List<String> adjacentVertices= new ArrayList<>();
	    
	    //Loads the adjacents vertices of the key
		for (Text val : values) {
		adjacentVertices.add(val.toString());
		}
		
		//Get the default pageRank
		Double defaultPageRank=1d/TOTAL_NUMBER_OF_NODES;
		
		//Create a vertice with the default page rank and his adjacents vertices
		Vertice vertice=new Vertice(defaultPageRank, adjacentVertices);
		
		//The key is the vertice id
		//write the key with his vertice
		context.write(key, new Text(vertice.toString()));
	}



}

