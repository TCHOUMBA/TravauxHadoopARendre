package cs.bigdata.Tutorial2;

import java.time.LocalDate;
import java.time.Year;

public class Tree {
	
	private String geoPoint;
	private String arrondissement;
	private String genre;
	private String espece;
	private String famille;
	private String anneePlantation;
	private String hauteur;
	private String circonference;
	private String adresse;
	private String nomCommun;
	private String variete;
	private String objectID;
	private String nom_ev;
	private Tree(String geoPoint, String arrondissement, String genre, String espece, String famille,
			String anneePlantation, String hauteur, String circonference, String adresse, String nomCommun,
			String variete, String objectID, String nom_ev) {
		super();
		this.geoPoint = geoPoint;
		this.arrondissement = arrondissement;
		this.genre = genre;
		this.espece = espece;
		this.famille = famille;
		this.anneePlantation = anneePlantation;
		this.hauteur = hauteur;
		this.circonference = circonference;
		this.adresse = adresse;
		this.nomCommun = nomCommun;
		this.variete = variete;
		this.objectID = objectID;
		this.nom_ev = nom_ev;
	}
	
	
	public static Tree fromLine(String line) {
		//get Tree's data form line
		String[] treeData=line.split(";");
		//create a tree
		return new Tree(treeData[0], 
				treeData[1],
				treeData[2], 
				treeData[3], 
				treeData[4], 
				treeData[5], 
				treeData[6], 
				treeData[7], 
				treeData[8], 
				treeData[9], 
				treeData[10], 
				treeData[11], 
				treeData[12]);
	}

	

	public String getGeoPoint() {
		return geoPoint;
	}

	public String getArrondissement() {
		return arrondissement;
	}

	public String getGenre() {
		return genre;
	}

	public String getEspece() {
		return espece;
	}

	public String getFamille() {
		return famille;
	}

	public String getAnneePlantation() {
		return anneePlantation;
	}

	public String getHauteur() {
		return hauteur;
	}

	public String getCirconference() {
		return circonference;
	}

	public String getAdresse() {
		return adresse;
	}

	public String getNomCommun() {
		return nomCommun;
	}

	public String getVariete() {
		return variete;
	}

	public String getObjectID() {
		return objectID;
	}

	public String getNom_ev() {
		return nom_ev;
	}
	
	

}
