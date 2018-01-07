package cs.bigdata.Lab2.pageranking;


import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs.bigdata.Lab2.Vertice;


public class PageRankMap extends Mapper<LongWritable, Text, Text, Text> {
	

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		String[] parts=StringUtils.split(value.toString());	

		//Load the vertice from the current line
		Vertice vertice=Vertice.fromLine(value.toString());

		//the key is the vertice id
		//write the key with is vertice
		context.write(new Text(parts[0]), new Text(vertice.toString()));

		//get the vertice's pageRank
		Double pageRank=vertice.getPageRank();

		//do nothing if the vertices has no adjacent  vertices
		if(vertice.getAdjancentVertices()==null) return; 
		
		//for each adjacent vertice
		for (String adjacentVerticeId:vertice.getAdjancentVertices()) {
			//Load  the adjacent vertice(neighboor)
			Vertice adjacentVertice=   new Vertice(0d,null);
			
			//propagate the page rank to the adjacent vertice
			double contributionPageRank=pageRank/(double)vertice.getAdjancentVertices().size();
			adjacentVertice.setPageRank(contributionPageRank);
			
			//the key is the the id of the adjacent vertices
			//write the key with is vertice 
			context.write(new Text(adjacentVerticeId), new Text(adjacentVertice.toString()));
		}

	}
}






