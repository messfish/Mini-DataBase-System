package PhysicalOperators;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Support.Mule;
import TableElement.DataType;
import TableElement.Tuple;

/**
 * This operator is used to scan the whole tables and fetch the 
 * whole tuples out. Notice the tuple should be a list of data.
 * @author messfish
 *
 */
public class ScanOperator extends Operator {

	private File file;
	private RandomAccessFile output;
	private FileChannel fc;
	private ByteBuffer buffer;
	private static final int NUM_OF_BYTES = 16384;
	// this is the number of bytes in a single page.
	private int numoftables; 
	// this variable stores the number of tables in the file.
	private Map<String, Mule> schema; // this map stores the schema.
	private int currentpoint; 
	// this index tells the current order of the tuples in a page.
	private int pagelimit;
	// this integer stores the number of tuples available in a page.
	private int index; // this integer stores the point in the page.
	private List<Integer> typelist;
	// this integer stores the index of each attributes in the table.
	private Tuple current; // this variable stores the current tuple.
	
	/**
	 * Constructor: this constructor consumes a file and stores 
	 * the file into the global objects. It will then retrieve the
	 * first page from the file and stores the schema of the table.
	 * @param file the file in the binary form.
	 */
	public ScanOperator(File file) {
		this.file = file;
		schema = new HashMap<>();
		typelist = new ArrayList<>();
		String temp = "";
		try {
			output = new RandomAccessFile(file, "r");
			fc = output.getChannel();
			buffer = readPage();
			/* Notice the format of the head file: the first byte is
			 * the number of tables in it. Followed by the name of the
			 * attributes and the type of that attribute. */
			numoftables = buffer.get(0);
			int index = 1, point = 0;
			/* the length of the name could not be 0, so when we encounter
			 * this, usually means we reach the end of the table list. */
			while(buffer.get(index)!=0) {
				int size = buffer.get(index);
				index++;
				StringBuilder sb = new StringBuilder();
				for(int i=0;i<size;i++) {
					char c = (char)buffer.get(index);
					sb.append(c);
					index++;
				}
				String get = sb.toString().split("\\.")[0];
				if(!temp.equals(get)) {
					typelist.add(-1);
					temp = get;
				}
				int datatype = buffer.get(index);
				index++;
				Mule mule = new Mule(point, datatype);
				point++;
				typelist.add(datatype);
				schema.put(sb.toString(), mule);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This method is used to get the next tuple available in the
	 * table. return null if there is no tuple left.
	 * @return the tuple or a null value.
	 */
	@Override
	public Tuple getNextTuple() {
		/* this indicates we need to fetch a new page from the disk. */
		if(currentpoint==pagelimit) {
			buffer = readPage();
			if(buffer==null) return null;
			pagelimit = buffer.getInt(0);
			currentpoint = 0;
			index = 4;
		}
		/* this byte indicates whether the tuple is valid, skip it. */
		index++;
		Tuple result = new Tuple(schema.size(), numoftables);
		int point = 0, IDpoint = 0;
		for(int i=0;i<typelist.size();i++) {
			int dummy = typelist.get(i);
			/* this means this is the order of the sub tuple. */
			if(dummy==-1){
				result.setTupleID(IDpoint, buffer.getLong(index));
				IDpoint++;
				index += 8;
				point--;
			}
			/* this means this is a long integer value. */
			else if(dummy==1) {
				DataType data = new DataType(buffer.getLong(index));
				result.setData(point, data);
				index += 8;
			}
			/* this means this is a string value. */
			else if(dummy==2) {
				int length = buffer.get(index);
				index++;
				StringBuilder sb = new StringBuilder();
				for(int j=0;j<length;j++) {
					char c = (char)buffer.get(index);
					index++;
					sb.append(c);
				}
				DataType data = new DataType(sb.toString());
				result.setData(point, data);				
			}
			/* this means this is a double value. It could be a time value
			 * , date value or just a plain double value.*/
			else {
				DataType data = new DataType(buffer.getDouble(index));
				result.setData(point, data);
				index += 8;
			}
			point++; // do not forget to increment the index of the tuple array!
		}
		currentpoint++;
		current = result;
		return result;
	}

	/**
	 * this method is used to reset the file pointer back to the starting point.
	 */
	@Override
	public void reset() {
		current = null;
		try {
			output = new RandomAccessFile(file, "r");
			fc = output.getChannel();
			buffer = readPage();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method is used to retrieve the schema from the table.
	 * @return a hash map with the attribute as the key and a combination
	 * of the index and the data type as the value.
	 */
	@Override
	public Map<String, Mule> getSchema() {
		return schema;
	}
	
	/**
	 * This abstract method is used to fetch the number of tables in
	 * the single operator.
	 * @return the number of tables in this operator.
	 */
	@Override
	public int getNumOfTables() {
		return numoftables;
	}
	
	/**
	 * This private method reads a page from the file and stores
	 * that page into the byte buffer.
	 * @return the byte buffer, null means nothing left to read.
	 */
	private ByteBuffer readPage() {
		ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
		int length = 0;
		try {
			length = fc.read(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(length==-1) return null;
		return buffer;
	}
	
	/**
	 * This method is used to get the current tuple.
	 * @return the current tuple.
	 */
	public Tuple getCurrentTuple() {
		return current;
	}

	/**
	 * This method is used to close the file out put stream.
	 */
	public void close() {
		try {
			output.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
