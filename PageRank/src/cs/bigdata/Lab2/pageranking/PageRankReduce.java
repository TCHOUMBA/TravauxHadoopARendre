package cs.bigdata.Lab2.pageranking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs.bigdata.Lab2.Counter;
import cs.bigdata.Lab2.Vertice;




public class PageRankReduce extends  Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		Vertice vertice = new Vertice(0d,null);
		boolean isDanglingNode=true;
		Double sumPageRank=0d;

		for (Text val : values) {
			//Load The vertice from the line
			Vertice currentVertice=Vertice.fromString(val.toString());
			
			//sum the page rank of all contributions
			if (currentVertice.isContribution()) {
				sumPageRank+=currentVertice.getPageRank();
			}
			else {
				//Load the original vertice
				vertice=currentVertice;
				
				//mark the original vertice as no dangling node
				isDanglingNode=false;
			}
		}

		//set the page rank of the vertice
		vertice.setPageRank(sumPageRank);
		
		//The key is the id  of the vertice
		//write the key with  is vertice
		context.write(new Text(key), new Text(vertice.toString()));
		
		if (isDanglingNode) {
			//increment de number of dangling nodes
			context.getCounter(Counter.DANGLING_NODES).increment(1L);
		}

	}


}

