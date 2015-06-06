package k_means;

public class Cluster {
	private String id; // 簇心编号 
	private int points; //有多少点属于这个簇心
	private Vector vector;
	
	public Cluster(String line){
		String[] strs = line.split(",",3);
		id= strs[0];
		points = Integer.parseInt(strs[1]);
		vector = new Vector(strs[2]);
	}

	@Override
	public String toString(){
		return "[ id : "+id +" , Points : "+ points+" , "+vector+"]";
	}
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getPoints() {
		return points;
	}

	public void setPoints(int points) {
		this.points = points;
	}

	public Vector getVector() {
		return vector;
	}

	public void setVector(Vector vector) {
		this.vector = vector;
	}

}
