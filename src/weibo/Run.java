package weibo;

import java.io.IOException;

public class Run {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		//getHotTopicData();
		getInfluencePersons();
		//ActivityDegree.getActivityDegree();
		//CityUserNum.getCityUserNum();
		//getGenderTopicPercentage();
		//TimeWeiboNum.getTimeWeiboNum();
	}
	
	//获取最具影响力的人物
	public static void getInfluencePersons() throws ClassNotFoundException, IOException, InterruptedException{
		//UserNum.getUserNum(); //获取用户总人数
		//InitProbability.getUserProbability();//获取用户初始影响力概率
		//RelationMR.getRelationData();//获取用户关注初始数据
		int len = 1;
		for(int i = 0; i < len; i++){
			if(i != 0)
				PageRankStep1.runStep1("/weibo/pageRankStep2_out" + (i - 1),"/weibo/pageRankStep1_out" + i);
			else
				PageRankStep1.runStep1("/weibo/userProba_out","/weibo/pageRankStep1_out" + i);
			//PageRankStep2.runStep2("/weibo/pageRankStep1_out" + i,"/weibo/pageRankStep2_out" + i);
		}
		//TopInfluenceMan.getTopInfluenceMan("/weibo/UserNameInfluence_out");
		//UserNameInfluence.getUserNameInfluence();
	}
	
	//获取热门话题
	public static void getHotTopicData() throws ClassNotFoundException, IOException, InterruptedException{
		//HotTopic.getHotTopic();
		HotTopicSort.getHotTopicSort();
	}
	
	public static void getGenderTopicPercentage() throws ClassNotFoundException, IOException, InterruptedException{
		//CombineGenderTopic.getCombineGenderTopic();
		GenderPercentage.getGenderPercentage();
	}
}
