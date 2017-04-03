package Support;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * This class is mainly used for modifying the human data into
 * the true human form.
 * @author messfish
 *
 */
public class HumanModify {

	/**
	 * This method is mainly used for converting the human data source
	 * file into the true human form file.
	 * @param origin the source file needs to be converted.
	 * @param destination the location of the new file.
	 * @return the newly converted file.
	 */
	public File convert(File origin, String destination) {
		File result = new File(destination);
		StringBuilder sb = new StringBuilder();
		int tupleID = 1;
		try {
			BufferedReader read = new BufferedReader(new FileReader(origin));
			String s = read.readLine();
			sb.append(s).append("\n");
			while((s=read.readLine())!=null) {
				sb.append(tupleID).append(" ");
				String[] array = s.split(",");
				for(int i=0;i<array.length;i++)
					sb.append(array[i].length()).append("/")
						.append(array[i] + " ");
				tupleID++;
				sb.append("\n");
			}
			BufferedWriter write = new BufferedWriter(new FileWriter(result));
			write.write(sb.toString());
			read.close();
			write.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
}
