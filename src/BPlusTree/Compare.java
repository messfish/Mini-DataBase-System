package BPlusTree;

import TableElement.DataType;

/**
 * This class is mainly used for comparing the key node which will be 
 * used by virtually all the class in this package.
 * @author messfish
 *
 */
public class Compare {

	/**
	 * This method is used to compare the two key arrays.
	 * @param key the search key used for comparison.
	 * @param candidate the key that is used for checking.
	 * @return an integer value interpreted as this: 1 means key is larger,
	 * 0 means they are equal, -1 means candidate is larger.
	 */
	protected int compareArray(DataType[] key, DataType[] candidate) {
		for(int i=0;i<key.length;i++) {
			int temp = key[i].compare(candidate[i]);
			if(temp!=0) return temp;
		}
		return 0;
	}
	
}
