package PhysicalOperators;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import Support.Catalog;
import Support.Mule;
import TableElement.Table;
import TableElement.Tuple;

/**
 * This class is used to handle the Cartesian product from several
 * table elements. Generally we use a pipeline theory to fetch all combinations.
 * @author messfish
 *
 */
public class CartesianOperator extends Operator {

	private ScanOperator[] scanlist;
	private int size; // this indicates how many attributes in the table.
	private Map<String, Mule> schema;
	
	/**
	 * Constructor: this constructor takes the map from the from list 
	 * and extract all the valuable data from the map.
	 * @param map the hash map originated from the from list.
	 * @param catalog the catalog that will be used.
	 */
	public CartesianOperator(Map<String, Table> map, Catalog catalog) {
		scanlist = new ScanOperator[map.size()];
		schema = new HashMap<>();
		int index = 0, previous = 0;
		String[] aliasarray = new String[map.size()];
		for(Map.Entry<String, Table> entry : map.entrySet()) {
			String locate = entry.getKey();
			String tablename = entry.getValue().getName();
			File scanfile = new File(catalog.getFileLocation(tablename));
			aliasarray[index] = locate;
			scanlist[index++] = new ScanOperator(scanfile);
			size += scanlist[index-1].getSchema().size();
		}
		for(int i=0;i<scanlist.length;i++) {
			Map<String, Mule> schema = scanlist[i].getSchema();
			for(Map.Entry<String, Mule> entry : schema.entrySet()) {
				String part = entry.getKey().split("\\.")[1];
				String combination = aliasarray[i] + "." + part;
				Mule mule = entry.getValue();
				mule.setIndex(previous + mule.getIndex());
				this.schema.put(combination, entry.getValue());
			}
			previous += schema.size();
		}
	}
	
	/**
	 * This method is used to fetch the next tuple available for the 
	 * operator. I use a pipeline method for this method which will
	 * be further explained in the helper function.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple result = new Tuple(size, scanlist.length);
		/* this indicates we are getting the first tuple. */
		if(scanlist[0].getCurrentTuple()==null) {
			int index = 0;
			for(int i=0;i<scanlist.length;i++) {
				Tuple tuple = scanlist[i].getNextTuple();
				for(int j=0;j<tuple.datasize();j++) {
					result.setData(index, tuple.getData(j));
					index++;
				}
				result.setTupleID(i, 1);
			}
			return result;
		}
		if(!pipeline(0, 0, result))
			return null;
		return result;
	}

	/**
	 * This method is used to reset the operator. Simply reset every
	 * scan operators in the array.
	 */
	@Override
	public void reset() {
		for(int i=0;i<scanlist.length;i++)
			scanlist[i].reset();
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
		return scanlist.length;
	}
	
	/**
	 * This method recursively go through the tuple list and when the
	 * during the post traverse, check whether the table is running out
	 * of the tuple. If yes, reset the operator and return a false value.
	 * If not, put the current tuple in the result and return a value.
	 * @param index the index of the scan operator in the array.
	 * @param start the starting point of the tuple.
	 * @param result the tuple used for storing result.
	 * @return the value shows whether there are tuples available.
	 */
	private boolean pipeline(int index, int start, Tuple result) {
		/* this means we reach the end, simply return a false value. */
		if(index==scanlist.length) return false;
		/* we return a false value to notify the former one that it 
		 * needs to get the next tuple. */
		if(!pipeline(index + 1, 
				start + scanlist[index].getSchema().size(), result)) {
			Tuple tuple = scanlist[index].getNextTuple();
			/* need to return false to notify the former one to get next tuple. */
			if(tuple==null) {
				scanlist[index].reset();
				tuple = scanlist[index].getNextTuple();
				setTuple(index, start, tuple, result);
				return false;
			}
			setTuple(index, start, tuple, result);
			return true;
		}
		Tuple tuple = scanlist[index].getCurrentTuple();
		setTuple(index, start, tuple, result);
		return true;
	}
	
	/**
	 * This method gets the data from the tuple and put them in the result
	 * tuple with index and start as the pointers.
	 * @param index the index of the scan operator in the array.
	 * @param start the starting point of the tuple.
	 * @param tuple the tuple used for reading.
	 * @param result the tuple used for storing result.
	 */
	private void setTuple(int index, int start, Tuple tuple, Tuple result) {
		for(int i=start;i<start+tuple.datasize();i++){
			result.setData(i, tuple.getData(i - start));
		}
		result.setTupleID(index, tuple.getTupleID(0));
	}

}
