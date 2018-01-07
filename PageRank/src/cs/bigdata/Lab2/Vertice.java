package cs.bigdata.Lab2;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class Vertice {

	private Double pageRank;
	private List<String> adjancentVertices;

	public Vertice(Double pageRank, List<String> adjancentVertices) {
		super();
		this.pageRank = pageRank;
		this.adjancentVertices = adjancentVertices;
	}
	public Double getPageRank() {
		return pageRank;
	}
	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}

	public List<String> getAdjancentVertices() {
		return adjancentVertices;
	}
	public void setAdjancentVertices(List<String> adjancentVertices) {
		this.adjancentVertices = adjancentVertices;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuilder builder= new StringBuilder();
		builder.append(String.valueOf(pageRank));
		
		if(!this.isContribution()) {
			builder.append("\t");
			builder.append(StringUtils.join(getAdjancentVertices().listIterator(),","));
		}
		return builder.toString();

	}

	public static Vertice fromLine(String line) {
		String[] parts=line.split("\t");
		List<String> adjacents=null;

		if (parts.length==3) {
			adjacents=Arrays.asList(parts[2].split(","));
		}

		return new Vertice(Double.valueOf(parts[1]), adjacents);

	}
	
	public static Vertice fromString(String verticeString) {
		String[] parts=verticeString.split("\t");
		List<String> adjacents=null;

		if (parts.length==2) {
			adjacents=Arrays.asList(parts[1].split(","));
		}

		return new Vertice(Double.valueOf(parts[0]), adjacents);

	}
	
	

	public boolean isContribution() {
		return adjancentVertices==null;
	}



}
