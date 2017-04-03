package BPlusTree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import TableElement.DataType;

/**
 * This class is the super class of both the index node and the leaf
 * node. Basically the difference between the leaf node and the index
 * node is the leaf node only contains a file that stores a list of 
 * key and the pointer to the tuple. The index node, on the other hand,
 * may have a file that contains the list of keys. In addition, it 
 * has a list of file directory that serves as the children of the 
 * node. For each page, it will be a binary file which has a storage
 * as 4KB. 
 * @author messfish
 *
 */
public abstract class Node {

	protected static final int NUM_OF_BYTES = 4096;
	protected String filelocation;
	protected List<DataType[]> keylist;
	protected int[] datatype;
	// remember, 1 means the data is long integer, 2 means the data is a 
	// string, 5 means the data is a double value.
	protected boolean isFirst = true;
	protected int numofkeys; 
	protected int firstkeyindex;
	
	/**
	 * Constructor: this constructor takes the path of the file as the
	 * parameter. Note the single file in the path is always named as
	 * "keylist". The format of the key file is as follows: The first
	 * integer is a value that indicates how many attributes are used as key.
	 * Followed by that is a list of the type of the data used
	 * for index. The rest of the content will be discussed in the
	 * index node and leaf node, respectively.
	 * @param filelocation the location of the file.
	 */
	public Node(String filelocation) {
		this.filelocation = filelocation;
		File file = new File(filelocation + "/keylist");
		keylist = new ArrayList<>();
		try {
			FileInputStream in = new FileInputStream(file);
			FileChannel fc = in.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
			int bufferlength = fc.read(buffer);
			numofkeys = buffer.getInt(0);
			datatype = new int[numofkeys];
			firstkeyindex = 4;
			for(int i=0;i<numofkeys;i++) {
				datatype[i] = buffer.getInt(firstkeyindex);
				firstkeyindex += 4;
			}
			while(bufferlength!=-1) {
				assignData(buffer);
				buffer = ByteBuffer.allocate(NUM_OF_BYTES);
				isFirst = false;
				bufferlength = fc.read(buffer);
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Constructor: this constructor is used to build a single node
	 * by scratch. assign the parameters to global variables.
	 * @param filelocation the location of the node in the file system.
	 * @param keylist the list of keys.
	 */
	public Node(String filelocation, List<DataType[]> keylist) {
		this.filelocation = filelocation;
		this.keylist = keylist;
		datatype = new int[keylist.get(0).length];
		for(int i=0;i<datatype.length;i++)
			datatype[i] = keylist.get(0)[i].getType();
	}
	
	/**
	 * This class is used for assigning the data into the list of
	 * data type as lists. Since the format of the IndexNode and the format
	 * of the LeafNode is not the same, I make this abstract.
	 * @param buffer the byte bufffer that store the data.
	 */
	protected abstract void assignData(ByteBuffer buffer);
	
	/**
	 * This method is used to check whether there is an overflow in
	 * the B+ Tree. Basically it checks whether the order of the BPlusTree
	 * times by 2 is smaller than the size of the key list.
	 * @return the boolean value shows whether there is an overflow.
	 */
	public boolean isOverflowed() {
		return keylist.size() > 2 * BPlusTree.KEY_LIMITS;
	}
	
	/**
	 * This method is used to check whether there is an underflow in the 
	 * B+ Tree. Basically it checks whether the order of the BPlusTree
	 * is greater than the size of the key list. Note for the root, we
	 * should allow the existence of underflow.
	 * @return the boolean value shows whether there is an underflow.
	 */
	public boolean isUnderflowed() {
		return keylist.size() < BPlusTree.KEY_LIMITS;
	}
	
	/**
	 * This method is used to write the new file. Note the format of the 
	 * file should follow the format specified in the constructor. It will
	 * be mentioned in the writePage() method implemented by the child classes.
	 */
	protected void writeFile() {
		File file = new File(filelocation + "/keylist");
		try {
			FileOutputStream out = new FileOutputStream(file);
			FileChannel fc = out.getChannel();
			int index = 0;
			boolean firstTime = true;
			while(index!=keylist.size()) {
				index = writePage(firstTime, index, fc);
				firstTime = false;
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This abstract method is used to write a single buffer page.
	 * It will be implemented differently by the two children.
	 * @param firstTime a parameter indicates whether this is the first time
	 * we used this method.
	 * @param index the index to start at the key list.
	 * @param fc the file channel.
	 * @return the index of the key list.
	 */
	protected abstract int writePage(boolean firstTime, int index,
									 FileChannel fc);
	
	/**
	 * This helper method is used to put the key into the key list.
	 * @param buffer the byte buffer that contains data.
	 * @param start the starting point of the key.
	 * @return the integer shows where the start should be after the reading.
	 */
	protected int assignKey(ByteBuffer buffer, int start) {
		DataType[] datalist = new DataType[numofkeys];
		for(int i=0;i<numofkeys;i++) {
			int increment = 8;
			if(datatype[i]==1) 
				datalist[i] = new DataType(buffer.getLong(start));
			else if(datatype[i]==5)
				datalist[i] = new DataType(buffer.getDouble(start));
			else if(datatype[i]==2) {
				int length = buffer.get(start);
				start++;
				StringBuilder sb = new StringBuilder();
				for(int j=0;j<length;j++) {
					sb.append((char)buffer.get(start));
					start++;
				}
				datalist[i] = new DataType(sb.toString());
				increment = 0;
			}
			start += increment;
		}
		return start;
	}
	
	/**
	 * This method is used to check whether the remaining space in the buffer
	 * page could hold the key.
	 * @param start the starting point in the buffer page.
	 * @param index the index that points the position of the key list.
	 * @return the boolean value shows whether we can put the key in it.
	 */
	protected boolean outofBound(int start, int index) {
		for(int i=0;i<datatype.length;i++) {
			if(datatype[i]==1||datatype[i]==5)
				start += 8;
			else if(datatype[i]==2) {
				String str = keylist.get(index)[i].getString();
				start += 1 + str.length();
			}
		}
		return start <= NUM_OF_BYTES;
	}
	
	/**
	 * This method is used to write the key elements back to the byte buffer.
	 * @param buffer the byte buffer that we put data in.
	 * @param start the starting point of the buffer page.
	 * @param index the index of the key list.
	 * @return the ending point of the key in the buffer page.
	 */
	protected int writeKey(ByteBuffer buffer, int start, int index) {
		for(int i=0;i<datatype.length;i++) {
			DataType data = keylist.get(index)[i];
			int increment = 8;
			if(datatype[i]==1)
				buffer.putLong(start, data.getLong());
			else if(datatype[i]==5)
				buffer.putDouble(start, data.getDouble());
			else if(datatype[i]==2) {
				String str = data.getString();
				buffer.put(start, (byte)str.length());
				start++;
				for(char c : str.toCharArray()) {
					buffer.put(start, (byte)c);
					start++;
				}
				increment = 0;
			}
			start += increment;
		}
		return start;
	}
	
}
