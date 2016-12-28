package hadoop_projects.mongo_hadoop;

import org.apache.pig.ExecType;

public class PigRunner {
	private static final boolean isLocal = false;

	public static void main(String[] args) {
		PigETL pigETL=null;
		try{
			pigETL=new PigETL(isLocal,ExecType.LOCAL);
			pigETL.loadData();
			System.out.println("Done");
			//pigETL.transformData();
		}catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
