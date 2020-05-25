package cps.prmr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtil {
	public static List<Path> getFiles(Configuration conf, String path) throws FileNotFoundException, IOException {
		List<Path> result = new ArrayList<Path>();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(path));
			for (FileStatus file : status) {

				if (!file.isDirectory()) {
					if (file.getLen() != 0) {
						result.add(file.getPath());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}
