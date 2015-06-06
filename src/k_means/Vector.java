package k_means;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class Vector implements Writable{
private List<Double> values = new ArrayList<Double>();
	
	public Vector(){}
	
	public Vector(String line){
		String[] strs = line.split(",");
		for(String str : strs){
			values.add(Double.parseDouble(str));
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(values.size());
		for(Double value : values){
			out.writeDouble(value);
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		values.clear();
		 int size  = in.readInt();
		 for(int i = 0 ; i< size ; i++){
			 double value = in.readDouble();
			 values.add(value);
		 }
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(int i = 0 ; i< values.size() -1 ; i++){
			sb.append(values.get(i)).append(",");
		}
		sb.append(values.get(values.size()-1));
		return sb.toString();
	}
	public List<Double> getValues() {
		return values;
	}
	public void setValues(List<Double> values) {
		this.values = values;
	}

	public void add(Vector v) {
		List<Double> vals = v.getValues();
		for(int i = 0 ; i< vals.size() ; i++){
			values.set(i,values.get(i)+vals.get(i));
		}
	}
	
	public void divide(int num){
		for(int i = 0 ; i<values.size();i++){
			values.set(i,values.get(i)/num);
		}
	}
}
