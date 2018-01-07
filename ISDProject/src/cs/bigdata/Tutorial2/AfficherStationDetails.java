package cs.bigdata.Tutorial2;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;



public class AfficherStationDetails {

	public static void main(String[] args) throws IOException {

		String fileSrc = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		final int NUMBER_OF_LINES_TO_IGNRORE=22;

		//Open The file
		InputStream in=new BufferedInputStream(fs.open(new Path(fileSrc)));

		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			//Skip the Lines to Ignore
			br.skip(NUMBER_OF_LINES_TO_IGNRORE);

			//Display HEADERS
			System.out.println(String.format("USAF \t NAME \t FIPS \t ALTITUDE "));

			// read line by line
			String line = br.readLine();

			while (line !=null){
				// Process of the current line
				String usafCode=line.substring(0, 6);
				String name=line.substring(13,42);
				String fips=line.substring(43,45);
				String altitude=line.substring(74,81);

				// Display station  data 
				System.out.println(String.format("%s \t %s \t %s \t %s", usafCode,name,fips,altitude));
			}
			// go to the next line
			line = br.readLine();


		}
		finally{
			//close the file
			in.close();
			fs.close();
		}



	}

}
