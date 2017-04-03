package Sorting;

import java.io.File;
import java.util.Map;

import PhysicalOperators.Operator;
import TableElement.Tuple;
import Support.Mule;

/**
 * This class is mainly used for debugging and testing: it will
 * help check whether there are data missing or the data in
 * the file is correctly sorted or not.
 * @author messfish
 *
 */
public class CheckSort {

	private Operator op;
	private int size; // this value checks how many tuples in the operator.
	private ExternalSort ex;
	private Map<String, Mule> schema;
	
	/**
	 * Constructor: this constructor is mainly used to generate an
	 * operator based on the file and the schema from the external sort class.
	 * @param sort the external sort that will be used for extraction.
	 */
	public CheckSort(ExternalSort sort) {
		ex = sort;
		schema = sort.getSchema();
		op = new TempOperator(sort.getResult(), sort.getSchema());
		size = 1;
	}
	
	/**
	 * This method is mainly used for comparing the tuples from start
	 * to finish. Note the first one should always not larger than the 
	 * tuple ahead of it. If we find an abnormal pair, print that out.
	 * @return the boolean value indicates whether the sort is valid.
	 */
	public boolean checkValid() {
		Tuple left = op.getNextTuple(), right = null;
		if(left == null) 
			return true;
		while((right=op.getNextTuple())!=null) {
			if(comparision(left,right)>0) {
				for(int i=0;i<left.datasize();i++) {
					left.getData(i).print();
					right.getData(i).print();
				}
				return false;
			}
			size++;
		}
		return true;
	}
	
	/**
	 * This method is used for picking out the file that might 
	 * generate an error.
	 * @param file the file that is used for checking.
	 * @return check whether the file is valid or not.
	 */
	public boolean checkValid(File file) {
		TempOperator temp = new TempOperator(file, schema);
		Tuple left = temp.getNextTuple(), right = null;
		if(left == null) 
			return true;
		while((right=temp.getNextTuple())!=null) {
			if(comparision(left,right)>0) {
				for(int i=0;i<left.datasize();i++) {
					left.getData(i).print();
					right.getData(i).print();
				}
				return false;
			}
		}
		return true;
	}
	
	/**
	 * This is the getter method of how many tuples in the operator.
	 * @return the number of tuples in the operator.
	 */
	public int getSize() {
		return size;
	}

	/**
	 * This method is used to compare the two tuples. Basically it is 
	 * the reuse of the comparision() method in external sort class.
	 * @param t1 one of the tuples to be compared.
	 * @param t2 one of the tuples to be compared.
	 * @return an integer to show which is bigger, 1 means t1 is bigger
	 * than t2, -1 means t1 is smaller than t2. 0 means they are equally
	 * the same.
	 */
	private int comparision(Tuple t1, Tuple t2) {
		return ex.comparison(t1, t2);
	}
	
}
