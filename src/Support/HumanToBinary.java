package Support;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import SmallSQLServer.Main;

/**
 * This class is used to convert the human readable file into
 * binary file. Note the format of the binary file should be like this:
 * The header of the file should contains a byte includes how many tables
 * are there in this file. Followed by the name of the table, the name
 * of the attribute and the type of the attribute. Notice for the string
 * variable, we need to append a byte of the number that indicates the 
 * length of the string before the string.
 * Next is the content of the file. At first is a 8 byte value that shows
 * how many tuples are there in the page. Next is the content of the tuple:
 * The first is the occurance of this tuple. Followed by that is the content
 * of each single table, the first one is the order of that table, followed
 * by the data of that table. Notice there should be a byte indicates the 
 * length of the string if the data type is a string.
 * @author messfish
 *
 */
public class HumanToBinary {
	
	private static final int NUM_OF_BYTES = 16384;
	// this is the number of bytes in a single page. Notice I set it
	// to 16KB, so this is the number of bytes for that size.
	private TimeConversion convert = new TimeConversion();
	private StringBuilder sb;
	
	/**
	 * This method is the main method that convert the human readable
	 * file into the binary readable file. Notice the format of the file
	 * should be followed from the definition of the class.
	 * @param file the human readable file.
	 * @param title the name of the table that needs to be converted.
	 * @return the file in the binary form.
	 */
	public File convert(File file, String title) {
		File result = new File(Main.getTest()+"/conversiontest/"+title+".b");
		List<Integer> typelist = new ArrayList<>();
		try {
			FileOutputStream out = new FileOutputStream(result);
			FileChannel fc = out.getChannel();
			BufferedReader read = new BufferedReader(new FileReader(file));
			String str = read.readLine();
			String[] array = str.split("\\s+");
			ByteBuffer buffer = writeHead(array, typelist);
			buffer.limit(buffer.capacity());
			buffer.position(0);
			fc.write(buffer);
			sb = new StringBuilder();
			while(true) {
				buffer = writePage(read, typelist);
				buffer.limit(buffer.capacity());
				buffer.position(0);
				fc.write(buffer);
				if(sb.length()==0) break;
			}
			read.close();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * This method is used to write the head of the file. Notice it should
	 * follow the format that is defined in the class.
	 * @param array the array that stores the schema of the table.
	 * @param typelist a list that stores the type of the data.
	 * @return the byte buffer which stores the data.
	 */
	private ByteBuffer writeHead(String[] array, List<Integer> typelist) {
		ByteBuffer result = ByteBuffer.allocate(NUM_OF_BYTES);
		result.put((byte)1);
		int index = 1;
		for(int i=0;i<array.length;i+=2) {
			String str = array[i];
			result.put(index, (byte)str.length());
			index++;
			for(char c : str.toCharArray()) {
				result.put(index, (byte)c);
				index++;
			}
			int type = Integer.parseInt(array[i+1]);
			result.put(index, (byte)type);
			typelist.add(type);
			index++;
		}
		return result;
	}
	
	/**
	 * This method is mainly used for writing the main page of the binary file.
	 * Notice it should follow the format defined in the class definition.
	 * @param read the buffered reader to get lines out from human file.
	 * @param sb the string that stores a temporary line.
	 * @param typelist the list that stores the type of the attribute.
	 * @return the byte buffer that contains the data written.
	 */
	private ByteBuffer writePage(BufferedReader read, List<Integer> typeList) {
		ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
		int numoftuples = 0, index = 4;
		/* this is the tuple that cannot be written to the former buffer.
		 * so we need to handle that in advance. */
		if(sb.length()!=0) {
			List<Byte> list = writeLine(index, sb.toString(), typeList);
			for(byte data : list) {
				buffer.put(index, data);
				index++;
			}
			numoftuples++;
		}
		String temp = null;
		try {
			while((temp=read.readLine())!=null) {
				sb = new StringBuilder(temp);
				List<Byte> list = writeLine(index, sb.toString(), typeList);
				if(list==null) break;
				for(byte data : list) {
					buffer.put(index, data);
					index++;
				}
				numoftuples++;
			}
			/* this means no more tuples left, I get the string builder to a 
			 * whole new one to let the caller methods know nothing left. */
			if(temp==null)
				sb = new StringBuilder();
			buffer.putInt(numoftuples);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return buffer;
	}
	
	/**
	 * This method is used to write the line in the buffer page. Notice
	 * it should follow the method in the class definition. If the 
	 * byte buffer could not handle that line, return null. Note that the 1
	 * is the long integer, 2 is the string value, 3 is the date value and
	 * 4 is the time value, 5 is the double integer.
	 * @param index the starting point of the byte buffer page.
	 * @param s the string that needs to be parsed.
	 * @param typeList the array that stores the type of data.
	 * @return an array list storing bytes, null means we have an overflow.
	 */
	private List<Byte> writeLine(int index, String s, List<Integer> typeList) {
		List<Byte> result = new ArrayList<>();
		/* this byte is used to check whether the tuple is valid. 
		 * since it is always valid, we append 1 to it. */
		result.add((byte)1);
		int point = 0, attribute = 0;
		long first = 0;
		while(s.charAt(point)!=' ') {
			first = first * 10 + (long)(s.charAt(point) - '0');
			point++;
		}
		storeData(first, result);
		point++;
		while(point<s.length()) {
			int nums = 0;
			while(point<s.length()&&s.charAt(point)!='/') {
				nums = nums * 10 + (int)(s.charAt(point) - '0');
				point++;
			}
			point++;
			String fetch = s.substring(point, point+nums);
			if(typeList.get(attribute)==1) {
				long data = Long.parseLong(fetch);
				storeData(data, result);
			}else if(typeList.get(attribute)==2) {
				result.add((byte)fetch.length());
				for(char c : fetch.toCharArray())
					result.add((byte)c);
			}else if(typeList.get(attribute)==3) {
				double change = convert.fromDateToNumber(fetch);
				long lng = Double.doubleToLongBits(change);
				storeData(lng, result);
			}else if(typeList.get(attribute)==4) {
				double change = convert.fromTimeToNumber(fetch);
				long lng = Double.doubleToLongBits(change);
				storeData(lng, result);
			}else if(typeList.get(attribute)==5) {
				double data = Double.parseDouble(fetch);
				long lng = Double.doubleToLongBits(data);
				storeData(lng, result);
			}
			/* this is mainly used for debugging. */
			else {
				System.out.println("You get an invalid type!");
			}
			point += nums + 1;
			attribute++;
		}
		/* this indicates the byte buffer cannot handle the line. */
		if(index + result.size() > NUM_OF_BYTES)
			return null;
		return result;
	}
	
	/**
	 * This method is used to convert the long integer as bytes and store
	 * them in the array list of byte integers.
	 * @param data the number needs to be converted.
	 * @param result the array list that stores a list of bytes.
	 */
	private void storeData(long data, List<Byte> result) {
		long[] temp = new long[8];
		for(int i=7;i>=0;i--) {
			temp[i] = data & 0xff;
			data >>>= 8;
		}
		for(int i=0;i<8;i++)
			result.add((byte)temp[i]);
	}
	
}
