package hadoop_projects.mongo_hadoop;

import java.io.IOException;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

public class PigETL {
	private PigServer pigServer;
	private static String projectRootPath = System.getProperty("user.dir");
	private static String regex = ".*please.*";
	private boolean isLocal;

	public PigETL(boolean islocal, ExecType ex) throws ExecException {
		this.isLocal = islocal;
		if (!islocal) {
			projectRootPath = String.format("%s%s", projectRootPath, "hadoop-serde");
		}

		String jarPathUDF = String.format("%s/%s/%s", projectRootPath, "pig-jars", "pigmodule.jar");
		String jarPathJson = String.format("%s/%s/%s", projectRootPath, "pig-jars", "json-simple-1.1.1.jar");
		String jarPathMon = String.format("%s/%s/%s", projectRootPath, "pig-jars", "mongo-java-driver-3.0.0.jar");
		String jarPathMonH = String.format("%s/%s/%s", projectRootPath, "pig-jars", "mongo-hadoop-core-2.0.1.jar");
		String jarPathMonP = String.format("%s/%s/%s", projectRootPath, "pig-jars", "mongo-hadoop-pig-2.0.1.jar");
		String jarPathJm = String.format("%s/%s/%s", projectRootPath, "pig-jars", "jackson-mapper-asl-1.9.13.jar");
		String jarPathJc = String.format("%s/%s/%s", projectRootPath, "pig-jars", "jackson-core-asl-1.9.13.jar");

		String jarPathPiggy = String.format("%s/%s/%s", projectRootPath, "pig-jars", "piggybank-0.14.0.jar");
		pigServer = new PigServer(ex);

		try {
			pigServer.registerJar(jarPathPiggy);
			pigServer.registerJar(jarPathUDF);
			pigServer.registerJar(jarPathJson);
			pigServer.registerJar(jarPathMon);
			pigServer.registerJar(jarPathMonH);
			pigServer.registerJar(jarPathJm);
			pigServer.registerJar(jarPathJc);
			pigServer.registerJar(jarPathMonP);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public void transformData() throws IOException {
		pigServer.registerQuery("mongoJson = LOAD './pig_out/first_collection.json' USING JsonLoader();");
		pigServer.registerQuery(
				"STORE mongoJson INTO 'mongodb://localhost:27017/test.tweets' USING com.mongodb.hadoop.pig.MongoInsertStorage;");

	}

	public void loadData() throws IOException {
		pigServer.registerQuery("raw = load 'raw_data.txt' as (str:chararray);");

		pigServer.registerQuery("C = filter raw by str matches '.*please.*';");

		if (!isLocal) {
			pigServer.registerQuery("STORE C into './pig_out/first_collection.json' USING JsonStorage();");
		} else {

			pigServer.registerQuery("STORE C into './pig_out/first_collection.json' USING JsonStorage();");

		}
	}

}
