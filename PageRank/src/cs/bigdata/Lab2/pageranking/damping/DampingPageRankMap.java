package cs.bigdata.Lab2.pageranking.damping;


import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs.bigdata.Lab2.Counter;
import cs.bigdata.Lab2.Vertice;


public class DampingPageRankMap extends Mapper<LongWritable, Text, Text, Text> {
	private Double DAMPING_FACTOR=0d;
	private Long TOTAL_NUMBER_OF_DANGLING_NODES=0L;
	private Long TOTAL_NUMBER_OF_NODES=0l;
	private int SCALING_FACTOR=0;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//get the damping factor
		DAMPING_FACTOR=context.getConfiguration().getDouble("DAMPING_FACTOR", 0d);

		//Get the total number of dangling nodes
		TOTAL_NUMBER_OF_DANGLING_NODES=context.getConfiguration().getLong("TOTAL_NUMBER_OF_DANGLING_NODES", 0L);

		//Get the total number of nodes
		TOTAL_NUMBER_OF_NODES=context.getConfiguration().getLong("TOTAL_NUMBER_OF_NODES", 1L);

		//Get the scaling factor
		SCALING_FACTOR=1000;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		String[] parts=StringUtils.split(value.toString());	

		//load the vertice from line
		Vertice vertice=Vertice.fromLine(value.toString());

		//Get the page rank of the vertice
		Double pageRank=vertice.getPageRank();

		//Get the teleportation term
		Double teleportation=(1-DAMPING_FACTOR)/TOTAL_NUMBER_OF_NODES;

		//Update the page rank with damping factor and dangling nodes
		Double pageRankWithDanglingAndDamping=DAMPING_FACTOR*((TOTAL_NUMBER_OF_DANGLING_NODES/TOTAL_NUMBER_OF_NODES)+pageRank);

		//Get the final page rank
		Double finalPageRank=teleportation+pageRankWithDanglingAndDamping;

		//update vertice with the final page rank
		vertice.setPageRank(finalPageRank);

		//the key is the id of the vertice
		//write the key with its vertice
		context.write(new Text(parts[0]), new Text(vertice.toString()));

		//Get the page rank diff
		double pageRankDiff=finalPageRank-pageRank;

		//scale the page rank
		int scaledPageRandDiff=Math.abs((int)pageRankDiff*SCALING_FACTOR);

		//update the page rank diff counter
		context.getCounter(Counter.PAGE_RANK_DIFF).increment(scaledPageRandDiff);


	}


}






