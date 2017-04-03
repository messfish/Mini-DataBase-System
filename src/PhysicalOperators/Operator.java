package PhysicalOperators;

import TableElement.DataType;
import TableElement.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import SmallSQLServer.Main;
import Support.Mule;
import Support.TimeConversion;

/**
 * This class is the top level of the whole operator class.
 * Notice there are two main abstract methods that needs to be
 * implemented and they are the fundamentals of the database methods:
 * The first one is the getNextTuple() and the second one is 
 * reset(). 
 * @author messfish
 *
 */
public abstract class Operator {
	
	private static final int NUM_OF_BYTES = 16384;
	private List<String> attributelist = new ArrayList<>();
	// this is used to store the list of attributes with the order
	// that is given from the schema.
	private TimeConversion convert = new TimeConversion();
	
	/**
	 * This abstract method is used to get the next valid tuple 
	 * from the table.
	 * @return the next valid tuple.
	 */
	public abstract Tuple getNextTuple();
	
	/**
	 * this abstract method is used to reset the pointer to the
	 * starting point of the table.
	 */
	public abstract void reset();
	
	/**
	 * This abstract method is used to get the schema of the table
	 * and store the result in a map, which has the string attribute
	 * as the key, a mule class which includes the index of the attribute
	 * and the data type of that attribute.
	 * @return a hash map includes the schema of the table.
	 */
	public abstract Map<String, Mule> getSchema();
	
	/**
	 * This abstract method is used to fetch the number of tables in
	 * the single operator.
	 * @return the number of tables in this operator.
	 */
	public abstract int getNumOfTables();
	
	/**
	 * This method is used to get all the tuples available and store them
	 * into a file. Notice we need to get the data that has the largest 
	 * length of the column and store that into the array. Return that array.
	 * @param index the 
	 */
	public int[] dump(int index) {
		File file = new File(Main.getOutput() + "/" + index);
		int[] result = new int[getSchema().size()];
		int[] datatype = new int[getSchema().size()];
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(writeHead(getSchema(), datatype, result)).append("\n");
			Tuple tuple = null;
			while((tuple=getNextTuple())!=null) 
				sb.append(writeLine(tuple, datatype, result)).append("\n");
			BufferedWriter write = new BufferedWriter(new FileWriter(file));
			write.write(sb.toString());
			write.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * This is the method that writes the schema of the table into
	 * the head line of the human readable file.
	 * @param schema the schema of the table.
	 * @param datatype the list of array that indicates the data type.
	 * @return the header of the table file as string.
	 */
	private String writeHead(Map<String, Mule> schema, int[] datatype, int[] result) {
		String[] attributearray = new String[schema.size()];
		for(Map.Entry<String, Mule> entry : schema.entrySet()) {
			Mule mule = entry.getValue();
			attributearray[mule.getIndex()] = entry.getKey();
			datatype[mule.getIndex()] = mule.getDataType();
			result[mule.getIndex()] = entry.getKey().length();
		}
		StringBuilder sb = new StringBuilder();
		for(String str : attributearray)
			sb.append(str).append(" ");
		return sb.deleteCharAt(sb.length() - 1).toString();
	}
	
	/**
	 * This method is used to write the line of the table file by 
	 * using the tuple in the attribute and the data type array.
	 * Note after the conversion to string. remember to compare the length
	 * with the length in the result and store the larger one.
	 * @param tuple the tuple that needs to be changed.
	 * @param datatype the array shows the type of the data.
	 * @param result the array shows the maximum length of the data.
	 * @return the parsed string.
	 */
	private String writeLine(Tuple tuple, int[] datatype, int[] result) {
		StringBuilder sb = new StringBuilder();
		sb.append(tuple.getTupleID(0)).append(" ");
		for(int i=0;i<tuple.datasize();i++) {
			DataType data = tuple.getData(i);
			String datastr = null;
			if(datatype[i]==1) 
				datastr = String.valueOf(data.getLong());
			else if(datatype[i]==2) 
				datastr = String.valueOf(data.getString());
			else if(datatype[i]==3) 
				datastr = convert.fromNumberToDate(data.getDouble());
			else if(datatype[i]==4) 
				datastr = convert.fromNumberToTime(data.getDouble());
			else if(datatype[i]==5) 
				datastr = String.valueOf(data.getDouble());
			sb.append(datastr.length()).append("/").append(datastr).append(" ");
			result[i] = Math.max(result[i], datastr.length());
		}
		return sb.deleteCharAt(sb.length() - 1).toString();
	}
	
	/**
	 * This method is mainly used for checking how many bytes it need to 
	 * store the whole tuple.
	 * @param tuple the tuple that will be used for checking.
	 * @return the number of bytes to store the tuple.
	 */
	public int checkSize(Tuple tuple) {
		int size = 0;
		for(int i=0; i<tuple.datasize();i++) {
			DataType data = tuple.getData(i);
			if(data.getType()==1) 
				size += 8;
			/* we need a byte to identify the length of the string. */
			else if(data.getType()==2)
				size += data.getString().length() + 1;
			else if(data.getType()==5)
				size += 8;
		}
		return size;
	}
	
	/**
	 * This method is mainly used for writing the tuple into the byte buffer.
	 * @param buffer the byte buffer used to put the data in.
	 * @param tuple the tuple that will be put into the byte buffer
	 * @param index the index shows the location to put in the byte buffer.
	 */
	public void writeTuple(ByteBuffer buffer, Tuple tuple, int index) {
		for(int i=0;i<tuple.datasize();i++) {
			DataType data = tuple.getData(i);
			if(data.getType()==1) {
				long number = data.getLong();
				buffer.putLong(index, number);
				index += 8;
			}else if(data.getType()==2) {
				String s = data.getString();
				buffer.put(index, (byte)s.length());
				index++;
				for(char c : s.toCharArray()) {
					buffer.put(index, (byte)c);
					index++;
				}
			}else if(data.getType()==5) {
				double number = data.getDouble();
				buffer.putDouble(index, number);
				index += 8;
			}
		}
	}
	
}
