package BPlusTree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import TableElement.DataType;

/**
 * This class mainly used to describe the leaf node. Note besides the
 * key list. There is another list that stores the pointer of actual values. 
 * Since we allow duplicates here, it should be a list of lists.
 * The format of the pointer is an array with 2 elements, with the first one
 * being the pointer to the file channel and second one is the index of the
 * tuple in the single page. Both of them are represented as integers.
 * @author messfish
 *
 */
public class LeafNode extends Node {

	protected List<List<int[]>> valuelist = new ArrayList<>();
	private int point, limit; 
	// this two variables are used to trace the index of the single element
	// list and the number of the single elements in the list. 
	private int valueindex = -1;
	// this method is used to trace the index of the value list.
	// notice when it is -1, it usually means the next thing inserted
	// to the buffer page is the key and the number of elements for that key.
	private Compare compare = new Compare();
	
	/**
	 * Constructor: this constructor extends the constructor from the Node
	 * class. The format of the leaf node should be as follows: the first
	 * one is the length of the table entries for a single key, Note it is 
	 * an integer so it would use 4 bytes. The second
	 * one is the actual key, followed by the list of table entries. 
	 * @param filelocation
	 */
	public LeafNode(String filelocation) {
		super(filelocation);
	}
	
	/**
	 * Constructor: this constructor is used to build a leaf node from 
	 * scratch. That means, it will take the key list and the value list
	 * to write the key file in the file location given.
	 * @param filelocation the location of the node in the file system.
	 * @param keylist the list of keys.
	 * @param valuelist the list of values.
	 */
	public LeafNode(String filelocation, List<DataType[]> keylist,
						List<List<int[]>> valuelist) {
		super(filelocation, keylist);
		this.valuelist = valuelist;
		writeFile();
	}

	/**
	 * This method is used to write the content from the byte buffer into 
	 * the key list and the value list. Note the value list is a list of 
	 * a list of pairs of integers which will be served as table entries,
	 * that means, the first index is the page entry and the second index
	 * is the buffer index entry. 
	 * @param buffer the byte buffer that contains data.
	 */
	@Override
	protected void assignData(ByteBuffer buffer) {
		int start = 0;
		if(isFirst) start = firstkeyindex;
		/* Note neither the number of keys nor the index of the table
		 * can have a value of 0, and both of them are integers. */
		while(start + 4 < NUM_OF_BYTES && buffer.getInt(start) != 0) {
			/* this means this is the end of a list. */
			if(point==limit) {
				limit = buffer.getInt(start);
				point = 0;
				start = assignKey(buffer, start + 4);
				valuelist.add(new ArrayList<>());
			}else {
				List<int[]> temp = valuelist.get(valuelist.size() - 1);
				int pageindex = buffer.getInt(start),
					bufferindex = buffer.getInt(start + 4);
				temp.add(new int[]{pageindex, bufferindex});
				point++;
			}
		}
	}

	/**
	 * This method is used to write the new file when there is 
	 * some change for the node. Basically it puts the elements of key list
	 * and value list which follow the format specified in the constructor.
	 * Also for a specific design, the number of elements for a key
	 * must be followed by an actual key. In other words, we should not
	 * separate them.
	 * @param firstTime a parameter indicates whether this is the first time
	 * we used this method.
	 * @param index the index to start at the key list.
	 * @param fc the file channel.
	 * @return the index of the key list.
	 */
	@Override
	protected int writePage(boolean firstTime, int index, FileChannel fc) {
		ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
		int start = 0;
		if(firstTime) {
			index = -1;
			buffer.putInt(0, datatype.length);
			start += 4;
			for(int i=0;i<datatype.length;i++) {
				buffer.putInt(start, datatype[i]);
				start += 4;
			}
		}
		while(index < keylist.size()) {
			if(valueindex==-1) {
				index++;
				if(!outofBound(start + 4, index))
					break;
				buffer.putInt(start, valuelist.get(index).size());
				start = writeKey(buffer, start + 4, index);
			}else {
				int[] temp = valuelist.get(index).get(valueindex);
				if(start + 8 > NUM_OF_BYTES)
					break;
				buffer.putInt(start, temp[0]);
				buffer.putInt(start + 4, temp[1]);
				start += 8;
				valueindex++;
				if(valueindex==valuelist.get(index).size())
					valueindex = -1;
			}
		}
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
	 * This method is used to insert the key into the node, after the
	 * insertion, the key should remain sorted.
	 * @param key the key which will be inserted.
	 * @param value the value which will be inserted.
	 */
	public void insertSorted(DataType[] key, int[] value) {
		int index = 0, state = 0;
		for(;index < keylist.size();index++) {
			state = compare.compareArray(key, keylist.get(index));
			if(state>=0) break;
		}
		/* note there are two conditions: we already have the key
		 * or we do not have that key. We should handle them separately. 
		 * here is the condition we need to insert a new key. */
		if(index==keylist.size()||state>0) {
			keylist.add(index, key);
			List<int[]> dummy = new ArrayList<>();
			dummy.add(value);
			valuelist.add(index, dummy);
		}
		/* this means we will insert values into an existed key. */
		if(state==0)
			valuelist.get(index).add(value);
	}

}
