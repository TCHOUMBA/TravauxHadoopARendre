package cs.bigdata.Lab2.tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class TfIdfReduce extends  Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		//get the number of documents
		int numberOfDocuments = 2;
		
		//Get the numbers of documents containing the key
		int docsPerWord = 0;
		
		Map<String, String> wordFrequences = new HashMap<String, String>();
		for (Text val : values) {
			//Get the doc , word count, and words per doc tuples
			String[] doc_wordCount_wordsPerDoc = val.toString().split("\t");
			
			//get the doc
			String doc=doc_wordCount_wordsPerDoc[0];
			
			//get the word count
			String wordCount=doc_wordCount_wordsPerDoc[1];
			
			//get the words per doc
			String wordsPerDoc=doc_wordCount_wordsPerDoc[2];
			
			//get the word frequence
			String wordFrequence=wordCount+"/"+wordsPerDoc;
			
			//update the docs per word
			docsPerWord++;
			
			//register the word frequence in  the doc
			wordFrequences.put(doc, wordFrequence);
		}
		for (String document : wordFrequences.keySet()) {
			String[] wordCount_wordsPerDoc = wordFrequences.get(document).split("/");
			//Get the word count
			String wordCount=wordCount_wordsPerDoc[0];
			
			//Get the words per doc
			String wordsPerDoc=wordCount_wordsPerDoc[1];
			
			//Get the term frequency 
			//Tf=word count in doc /words Per doc
			double tf = Double.valueOf(Double.valueOf(wordCount)
					/ Double.valueOf(wordsPerDoc));

			//Get the interse document frequency 
			//idf= numbers of documents/ docs per word
			double idf = (double) numberOfDocuments / (double) docsPerWord;

			//given that log(10) = 0, just consider the term frequency in documents
			double tfIdf = numberOfDocuments == docsPerWord ?
					tf : tf * Math.log10(idf);

			//Get the word
			String word=key.toString();
			
			context.write(new Text(word+"\t"+document),new DoubleWritable(tfIdf));
		}


	}
}

