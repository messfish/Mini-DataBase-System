package TableElement;

import java.util.List;

/**
 * This class is used to define a collection of data types and
 * present some methods and constructors for manipulating it.
 * @author messfish
 *
 */
public class Tuple {

	private DataType[] datalist; // this array represents a tuple.
	private long[] tupleIDlist; // this array stores a list of tuple ID.
	
	/**
	 * Constructor: this constructor is used to generate a data list
	 * which has the length given the argument.
	 * @param length the length of the data list.
	 */
	public Tuple(int length, int size) {
		datalist = new DataType[length];
		tupleIDlist = new long[size];
	}
	
	/**
	 * this method is the getter method of the data from the single index.
	 * @param index the index used to retrieve the data.
	 * @return the data at the given index.
	 */
	public DataType getData(int index) {
		return datalist[index];
	}
	
	/**
	 * This method is the setter method of the data list.
	 * @param index the index where we set the data.
	 * @param data the data that will be settled.
	 */
	public void setData(int index, DataType data) {
		datalist[index] = data;
	}
	
	/**
	 * this method is the getter method of the tupleID from the single index.
	 * @param index the index used to retrieve the tupleID.
	 * @return the tupleID at the given index.
	 */
	public long getTupleID(int index) {
		return tupleIDlist[index];
	}
	
	/**
	 * This method is the setter method of the tupleID list.
	 * @param index the index where we set the tupleID.
	 * @param tupleID the tupleID that will be settled.
	 */
	public void setTupleID(int index, long tupleID) {
		tupleIDlist[index] = tupleID;
	}
	
	/**
	 * this method returns the length of the data list.
	 * @return the length of the data list.
	 */
	public int datasize() {
		return datalist.length;
	}
	
	/**
	 * This method is used to combine multiple tuples into one ID.
	 * @param tupleID the tupleID needs to be assigned.
	 */
	public void resetTupleID(long tupleID) {
		tupleIDlist = new long[1];
		tupleIDlist[0] = tupleID;
	}
	
	/**
	 * This method is used to check whether two tuples are equal.
	 * Notice the tuple in the parameter could not be null.
	 * @param that the tuple that is used for checking equality.
	 * @return the boolean value shows whether they are equal or not.
	 */
	public boolean isEqual(Tuple that) {
		if(this.datalist.length!=that.datalist.length)
			return false;
		for(int i=0;i<this.datasize();i++) {
			DataType data1 = this.getData(i);
			DataType data2 = that.getData(i);
			if(data1.compare(data2)!=0) 
				return false;
		}
		return true;
	}
	
	/**
	 * This method is used to check whether two tuples are equal, based
	 * on the attributes in the array list. Notice the tuple in the 
	 * parameter could not be null.
	 * @param that the tuple that is used for checking equality.
	 * @param list the list that stores the index of attributes to be checked.
	 * @return the boolean value shows whether that they are equal or not.
	 */
	public boolean isEqual(Tuple that, List<Integer> list) {
		if(this.datalist.length!=that.datalist.length)
			return false;
		for(int i=0;i<this.datasize();i++) {
			if(list.indexOf(i)!=-1) {
				DataType data1 = this.getData(i);
				DataType data2 = that.getData(i);
				if(data1.compare(data2)!=0) 
					return false;
			}
		}
		return true;
	}
	
}
