package cs.bigdata.Tutorial2;


import java.io.*;
import java.time.LocalDate;
import java.time.Year;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class AfficherArbreDetails {

	public static void main(String[] args) throws IOException {
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in=new BufferedInputStream(fs.open(new Path(args[0])));


		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			//Skip the First Line Which represents the Headers
			br.skip(1);
			
			// read line by line
			String line = br.readLine();
			
			//Display  HEADERS
			System.out.println("YEAR \t HEIGHT");

			while (line !=null){
				//Load a Tree
				Tree tree=Tree.fromLine(line);

				//Display  year and height of the tree
				System.out.println(String.format("%s \t %s", tree.getAnneePlantation(),tree.getHauteur()));
				
				// go to the next line
				line = br.readLine();

			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}



	}

}
