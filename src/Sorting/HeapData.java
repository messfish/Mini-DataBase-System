package Sorting;

import TableElement.Tuple;

/**
 * This class is mainly used for the data type in the priority
 * queue since we need to know which operator it belongs to when
 * we fetch the smallest tuple from the heap.
 * @author messfish
 *
 */
public class HeapData {

	private int index;
	private Tuple tuple;
	
	/**
	 * Constructor: this constructor is mainly used for assigning
	 * the arguments to the global variable, respectively.
	 * @param index the index of the tuple
	 * @param tuple the tuple that stores the data.
	 */
	public HeapData(int index, Tuple tuple) {
		this.index = index;
		this.tuple = tuple;
	}
	
	/**
	 * This is the getter method of the index.
	 * @return the index.
	 */
	public int getIndex() {
		return index;
	}
	
	/**
	 * This is the getter method of the tuple.
	 * @return the tuple.
	 */
	public Tuple getTuple() {
		return tuple;
	}
	
}
