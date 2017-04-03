package Support;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * This class is used for debugging: it checks whether the two files has the 
 * same content.
 * @author messfish
 *
 */
public class checkFile {
	
	private static final int NUM_OF_BYTES = 16384;

	/**
	 * This method is used to check whether two files are equal.
	 * @param file1 one of the files to be checked.
	 * @param file2 one of the files to be checked.
	 * @return the boolean values shows whether two files are equal.
	 */
	@SuppressWarnings("resource")
	public boolean checkHumanEqual(File file1, File file2) {
		String str1 = null, str2 = null;
		try {
			BufferedReader buff1 = new BufferedReader(new FileReader(file1));
			BufferedReader buff2 = new BufferedReader(new FileReader(file2));
			/* notice we should use an '&' to prevent the case when str1
			 * is null and jump out, the rest part is never called! */
			while((str1=buff1.readLine())!=null&(str2=buff2.readLine())!=null) {
				str1 = str1.trim();
				str2 = str2.trim();
				if(!str1.equals(str2))
					return false;
			}
			buff1.close();
			buff2.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return str1 == null && str2 == null;
	}
	
	/**
	 * This method is used to check whether two binary files are equal.
	 * @param file1 one of the files to be checked.
	 * @param file2 one of the files to be checked.
	 * @return the boolean values shows whether two files are equal.
	 */
	@SuppressWarnings("resource")
	public boolean checkBinaryEqual(File file1, File file2) {
		ByteBuffer buff1 = ByteBuffer.allocate(NUM_OF_BYTES);
		ByteBuffer buff2 = ByteBuffer.allocate(NUM_OF_BYTES);
		int state1 = 0, state2 = 0;
		try {
			FileOutputStream out1 = new FileOutputStream(file1);
			FileOutputStream out2 = new FileOutputStream(file2);
			FileChannel fc1 = out1.getChannel();
			FileChannel fc2 = out2.getChannel();
			while(true) {
				state1 = fc1.read(buff1);
				state2 = fc2.read(buff2);
				if(state1==-1||state2==-1)
					break;
				if(!checkEqual(buff1, buff2)) 
					return false;
				buff1 = ByteBuffer.allocate(NUM_OF_BYTES);
				buff2 = ByteBuffer.allocate(NUM_OF_BYTES);
			}
			out1.close();
			out2.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return state1 == -1 && state2 == -1;
	}
	
	/**
	 * This method is used to check whether two byte buffers are equal.
	 * It will check the content of the two byte by byte.
	 * @param buff1 one of the byte buffer to be checked.
	 * @param buff2 one of the byte buffer to be checked.
	 * @return a boolean value shows whether they are equal or not.
	 */
	private boolean checkEqual(ByteBuffer buff1, ByteBuffer buff2) {
		for(int i=0;i<NUM_OF_BYTES;i++) {
			if(buff1.get(i)!=buff2.get(i))
				return false;
		}
		return true;
	}
	
}
