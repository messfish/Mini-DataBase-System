package BPlusTree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import TableElement.DataType;

/**
 * This is the index node that stores the list of keys and a list of 
 * child entries. 
 * @author messfish
 *
 */
public class IndexNode extends Node {
	
	/**
	 * Constructor: this constructor extends the constructor from the 
	 * Node class. Note here is the format of the index node: Besides
	 * from the first two bytes specified in the node class, there is
	 * a number that tells how many keys are there in the key list, followed
	 * by the actual keys. Note for the String type, we need to attach one
	 * byte that indicates the length of the string. Also, for each page,
	 * the first four bytes indicates how many keys are there in the single
	 * page.
	 * @param filelocation the location of the file.
	 */
	public IndexNode(String filelocation) {
		super(filelocation);
	}

	/**
	 * Constructors: this constructor is used to write a new 
	 * @param location
	 * @param keylist
	 */
	public IndexNode(String location, List<DataType[]> keylist) {
		super(location, keylist);
		writeFile();
	}

	/**
	 * This is the method that extends from the Node class: fetch the
	 * desired data out and 
	 */
	@Override
	protected void assignData(ByteBuffer buffer) {
		int start = 0;
		/* for the first page, there is a flag shows whether the node is
		 * index node or not, and an integer shows the type of the data.
		 * As a result, we should start from 8. */
		if(isFirst) start = firstkeyindex;
		int numberofkeys = buffer.getInt(start);
		start += 4;
		for(int i=0;i<numberofkeys;i++) 
			start = assignKey(buffer, start);
	}
	
	/**
	 * This method is used to write a single page of the file.
	 * It return the index of the array list which the next 
	 * Page should follow.
	 * @param firstTime this variable is used to indicate whether this
	 * is the first time we call the method.
	 * @param fc the file channel that used to write the data.
	 * @return the next index to start on the key list.
	 */
	protected int writePage(boolean firstTime, int index, FileChannel fc) {
		int start = 4, numofkeys = 0, keyindex = 0;
		ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
		if(firstTime) {
			buffer.putInt(0, datatype.length);
			for(int i=0;i<datatype.length;i++) {
				buffer.putInt(start, datatype[i]);
				start += 4;
			}
			start += 4;
			keyindex = 4 + datatype.length * 4;
		}
		while(index < keylist.size()) {
			if(outofBound(start,index))
				break;
			start = writeKey(buffer,start,index);
			index++;
			numofkeys++;
		}
		buffer.putInt(keyindex, numofkeys);
		buffer.limit(buffer.capacity());
		buffer.position(0);
		try {
			fc.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return index;
	}
	
	/**
	 * This method is used to get the child by using the index 
	 * @param index the index of the child list.
	 * @param isleaf tells whether the child is leaf or not.
	 * @return the new Node which is the child from the given index.
	 */
	protected Node getChild(int index, boolean isLeaf) {
		String path = filelocation + "/" + index;
		if(isLeaf) return new LeafNode(path);
		else return new IndexNode(path);
	}
	
	/**
	 * This method is used to insert a key into the key list by
	 * using the index specified.
	 * @param key the key that will be inserted.
	 * @param index the index where we insert the key.
	 */
	public void insertSorted(DataType[] key, int index) {
		keylist.add(index, key);
	}
	
}
