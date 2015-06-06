package pageRank;

import java.io.IOException;

import org.bigdata.util.HadoopUtil;

public class Run {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		for(int i = 0; i < 10; i++){
			First.runFirst();
			
			HadoopUtil.delete("/pageRank_in_p");

			Second.runSecond();
			
			HadoopUtil.delete("/pageRank_out");
		}
	}
}
