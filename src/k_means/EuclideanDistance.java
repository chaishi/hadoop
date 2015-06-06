package k_means;


public class EuclideanDistance implements Distance{

	@Override
	public  double getDistance(Vector v1, Vector v2) {
		double sum = 0.0;
		for(int i = 0 ; i< v1.getValues().size() ; i++){
			sum += Math.pow(v1.getValues().get(i) - v2.getValues().get(i) , 2);
		}
		return Math.sqrt(sum);
	}

}

