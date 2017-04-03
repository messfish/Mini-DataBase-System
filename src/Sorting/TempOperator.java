package Sorting;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

import PhysicalOperators.Operator;
import Support.Mule;
import TableElement.DataType;
import TableElement.Tuple;

/**
 * This class is just similar to the Scan Operator. Except this
 * time, we do not have the header page. Also, we do not include
 * the tupleID and the valid byte to save the space. After all, this
 * operator is mainly used for sorting.
 * @author messfish
 *
 */
public class TempOperator extends Operator {

	private Map<String, Mule> schema;
	private FileInputStream in; // note for reading, we need to use this!
	private FileChannel fc;
	private ByteBuffer buffer;
	private int[] datatypearray;
	private int tupleindex, limit, index;
	private static final int NUM_OF_BYTES = 16384;
	private File file;
	
	/**
	 * Constructor: this constructor is mainly used for extracting pages
	 * from the file and fetch the tuple one by one. 
	 * @param file the file that will be used for extracting information.
	 * @param op the operator that will provide the schema.
	 */
	public TempOperator(File file, Map<String, Mule> schema) {
		this.file = file;
		this.schema = schema;
		datatypearray = new int[schema.size()];
		for(Map.Entry<String, Mule> entry : schema.entrySet()) {
			Mule mule = entry.getValue();
			datatypearray[mule.getIndex()] = mule.getDataType();
		}
		try {
			in = new FileInputStream(file);
			fc = in.getChannel();
			buffer = readPage();
			tupleindex = 0;
			if(buffer!=null)
				limit = buffer.getInt(0);
			index = 4;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method is used to get the next tuple available in the operator
	 * if we cannot find the tuple, simply return null.
	 * @return the next tuple available.
	 */
	@Override
	public Tuple getNextTuple() {
		if(tupleindex==limit) {
			buffer = readPage();
			if(buffer==null)
				return null;
			tupleindex = 0;
			limit = buffer.getInt(0);
			index = 4;
		}
		/* since we do not need a tupleID, we set the second parameter 0. */
		Tuple result = new Tuple(datatypearray.length, 0);
		for(int i=0;i<datatypearray.length;i++) {
			DataType data = null;
			if(datatypearray[i]==1) {
				data = new DataType(buffer.getLong(index));
				index += 8;
			}else if(datatypearray[i]==2) {
				int size = buffer.get(index);
				index++;
				StringBuilder sb = new StringBuilder();
				for(int j=0;j<size;j++) {
					char c = (char)buffer.get(index);
					sb.append(c);
					index++;
				}
				data = new DataType(sb.toString());
			}else {
				data = new DataType(buffer.getDouble(index));
				index += 8;
			}
			result.setData(i, data);
		}
		tupleindex++;
		return result;
	}

	/**
	 * This method is used to reset the pointer back to the starting point.
	 */
	@Override
	public void reset() {
		try {
			in = new FileInputStream(file);
			fc = in.getChannel();
			buffer = readPage();
			tupleindex = 0;
			if(buffer!=null)
				limit = buffer.getInt(0);
			index = 4;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * This is the getter method of the schema of the operator.
	 * @return the schema of the operator.
	 */
	@Override
	public Map<String, Mule> getSchema() {
		return schema;
	}

	/**
	 * This method is mainly used for getting the number of tables 
	 * available in the operator.
	 * @return the number of tables in the operator.
	 */
	@Override
	public int getNumOfTables() {
		return 1;
	}
	
	/**
	 * This method is mainly used for closing the operator when we do 
	 * not need to extract the tuple out anymore.
	 */
	public void close() {
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This method is used for reading a page from the file and
	 * store the content in the byte buffer. return null if we 
	 * cannot find any.
	 * @return the byte buffer that stores the content of the page.
	 */
	private ByteBuffer readPage() {
		ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
		int length = 0;
		try {
			length = fc.read(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(length == -1)
			return null;
		return buffer;
	}

}

