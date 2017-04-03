package PhysicalOperators;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import SQLExpression.ColumnNode;
import SQLExpression.Expression;
import SmallSQLServer.Main;
import Sorting.ExternalSort;
import Sorting.TempOperator;
import Support.Mule;
import TableElement.DataType;
import TableElement.Tuple;

/**
 * This class is mainly used for grouping the attributes that has the 
 * same value. It will be used for the having expression.
 * @author messfish
 *
 */
public class GroupByOperator extends Operator {

	private Operator op;
	private TempOperator operator;
	private List<Map.Entry<String, Mule>> residuelist;
	// this list is used to store the attributes that does not appear
	// in the attribute list. It will be used for the statistics.
	private List<Integer> groupindexlist;
	private static final int NUM_OF_BYTES = 16384;
	private Tuple tempstore;
	// this variable is used for storing the tuple that cannot be fit
	// into the current byte buffer page.
	private Tuple dummystore;
	// this variable is used for storing the tuple that will be compared
	// for equalness when we execute the writeTuple() method.
	private Map<String, Mule> schema;
	private int tupleID;
	
	/**
	 * Constructor: This constructor is used to set the operator
	 * to the global variables and use the external sort to sort
	 * the operators. Generate a TempOperator to get the tuples out.
	 * Note we need to perform some statistics for the data that
	 * will be handy for both projecting the aggregated columns out
	 * and used for the having clauses.
	 * @param op
	 * @param grouplist
	 */
	public GroupByOperator(Operator op, List<String> grouplist) {
		tupleID = 1;
		groupindexlist = new ArrayList<>();
		residuelist = new ArrayList<>();
		for(String s: grouplist) 
			groupindexlist.add(op.getSchema().get(s).getIndex());
		for(Map.Entry<String, Mule> entry : op.getSchema().entrySet()) {
			if(grouplist.indexOf(entry.getKey())==-1)
				residuelist.add(entry);
		}
		schema = new HashMap<>();
		buildSchema(grouplist);
		this.op = op;
		/* we need to change the list of strings to list of expressions
		 * first so it could be used for the external sort. */
		List<Expression> expressionlist = new ArrayList<>(grouplist.size());
		for(String s : grouplist) 
			expressionlist.add(new ColumnNode(s));
		ExternalSort ex = new ExternalSort(op, expressionlist, 1);
		TempOperator temp = new TempOperator(ex.getResult(), op.getSchema());
		File file = getStatistics(temp);
		operator = new TempOperator(file, schema);
	}

	/**
	 * This method is used to fetch the next tuple from the table.
	 * @return the next valid tuple.
	 */
	@Override
	public Tuple getNextTuple() {
		return operator.getNextTuple();
	}

	/**
	 * This method is used to reset the operator back to the starting point.
	 */
	@Override
	public void reset() {
		operator.reset();
	}

	/**
	 * This method returns the schema of the operator.
	 * @return the schema of the operator.
	 */
	@Override
	public Map<String, Mule> getSchema() {
		return schema;
	}

	/**
	 * This method is used to return the number of tables in the operator.
	 * Here, I simply return 1 since the tables are merged after this operator.
	 * @return the number of tables.
	 */
	@Override
	public int getNumOfTables() {
		return 1;
	}
	
	/**
	 * This method is used to close the file stream in the operator.
	 */
	public void close() {
		operator.close();
	}
	
	/**
	 * This method is used for building the schema of the operator.
	 * The order should be like this: First are the elements from
	 * the group list, next is the counting of the number of tuples
	 * and the number of distinct tuples. Next are the aggregate 
	 * functions of each attributes that does not appear in the group
	 * list. Store the results in the schema global variable.
	 * @param grouplist the list of attributes used for grouping.
	 */
	private void buildSchema(List<String> grouplist) {
		for(int i=0;i<grouplist.size();i++) {
			Mule mule = op.getSchema().get(grouplist.get(i));
			mule.setIndex(i);
			schema.put(grouplist.get(i), mule);
		}
		int index = grouplist.size();
		schema.put("COUNT(*)", new Mule(index, 1));
		schema.put("COUNT(DISTINCT*)", new Mule(index + 1, 1));
		index += 2;
		for(Map.Entry<String, Mule> entry : residuelist) {
			String attribute = entry.getKey();
			int type = entry.getValue().getDataType();
			if(type==1||type==5) {
				schema.put("SUM("+attribute+")", new Mule(index, type));
				schema.put("AVG("+attribute+")", new Mule(index + 1, type));
				index += 2;
			}
			schema.put("MAX("+attribute+")", new Mule(index, type));
			schema.put("MIN("+attribute+")", new Mule(index + 1, type));
			schema.put("COUNT("+attribute+")", new Mule(index + 2, type));
			index += 3;
		}
	}
	
	/**
	 * This method is used to get all kinds of statistics available for
	 * the attributes that does not appear in the group by array list.
	 * Note in addition, we will count the number of tuples and the the
	 * number of distinct tuples for the grouped tuples. For the attributes
	 * which types are numeric, we have five aggregate options: COUNT
	 * SUM MAX MIN AVG. But for the attributes that are not numeric, 
	 * only three aggregate options are available: COUNT MAX MIN.
	 * @param temp the operator that will be used to extract tuples.
	 * @return the file that contains the statistics.
	 */
	private File getStatistics(TempOperator temp) {
		File result = new File(Main.getTemp() + "/statistics");
		try {
			FileOutputStream out = new FileOutputStream(result);
			FileChannel fc = out.getChannel();
			ByteBuffer buffer = null;
			while((buffer=writePage(temp))!=null) {
				buffer.limit(buffer.capacity());
				buffer.position(0);
				fc.write(buffer);
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * This method is used for generating a buffer page.
	 * @param temp the operator used for extracting data out.
	 * @return the buffer page that will be written by file channel.
	 */
	private ByteBuffer writePage(TempOperator temp) {
		ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
		int index = 4, numoftuples = 0;
		if(tempstore != null) {
			writeTuple(buffer, tempstore, index);
			index += checkSize(tempstore);
			numoftuples++;
		}
		dummystore = temp.getNextTuple(); 
		while(true) {
			Tuple tuple = getTuple(temp);
			int size = checkSize(tuple);
			if(index + size > NUM_OF_BYTES) {
				tempstore = tuple;
				break;
			}
			numoftuples++;
		}
		buffer.putInt(0, numoftuples);
		return buffer;
	}
	
	/**
	 * This method is used for generating the single tuple available
	 * which will be written to the ByteBuffer.
	 * @param temp the operator that will be used for extracting data.
	 * @return the tuple that contains all the data needed.
	 */
	private Tuple getTuple(TempOperator temp) {
		Tuple result = new Tuple(getSchema().size(), getNumOfTables());
		constructTuple(temp, result);
		Tuple proceed = temp.getNextTuple();
		long numoftuples = 1, numofdistuples = 1;
		while(proceed!=null&&dummystore.isEqual(proceed, groupindexlist)) {
			numoftuples++;
			if(!dummystore.isEqual(proceed)) {
				numofdistuples++;
				dummystore = proceed;
			}
			changeTuple(temp,proceed,result);
			proceed = temp.getNextTuple();
		}
		result.setData(groupindexlist.size(), new DataType(numoftuples));
		result.setData(groupindexlist.size()+1, new DataType(numofdistuples));
		setlast(result, numoftuples);
		result.setTupleID(0, tupleID);
		tupleID++;
		dummystore = proceed;
		return result;
	}
	
	/**
	 * This question sets the initial data for the result tuple.
	 * Notice for the aggregation, the order is: SUM, AVG, MAX, MIN, COUNT. 
	 * @param temp the operator that pass values.
	 * @param result the result tuple that stores the whole data.
	 */
	private void constructTuple(TempOperator temp, Tuple result) {
		for(Map.Entry<String, Mule> entry : temp.getSchema().entrySet()) {
			String attribute = entry.getKey();
			DataType data = dummystore.getData(entry.getValue().getIndex());
			if(schema.containsKey(attribute)) 
				result.setData(schema.get(attribute).getIndex(), data);
		}
		int start = groupindexlist.size() + 2;
		for(Map.Entry<String, Mule> entry : residuelist) {
			DataType data = dummystore.getData(entry.getValue().getIndex());
			int type = entry.getValue().getDataType();
			if(type==1||type==5) {
				result.setData(start, data);
				result.setData(start + 1, data);
				start += 2;
			}
			result.setData(start, data);
			result.setData(start + 1, data);
			start += 3;
		}
	}
	
	/**
	 * This method is used for the comparison between the two tuples and
	 * store the desired data into the result tuple.
	 * Notice for the aggregation, the order is: SUM, AVG, MAX, MIN, COUNT.
	 * @param temp the operator used for extracting data.
	 * @param proceed the tuple used for comparison.
	 * @param result the result tuple used for storing the desired data.
	 */
	private void changeTuple(TempOperator temp, Tuple proceed, Tuple result) {
		int start = groupindexlist.size() + 2;
		for(Map.Entry<String, Mule> entry : residuelist) {
			int index = temp.getSchema().get(entry.getKey()).getIndex();
			DataType paradata = proceed.getData(index);
			int type = entry.getValue().getDataType();
			if(type==1||type==5) {
				result.setData(start, result.getData(start).add(paradata));
				result.setData(start+1, result.getData(start+1).add(paradata));
				start += 2;
			}
			DataType target = result.getData(start).compare(paradata) >= 0 ?
							  result.getData(start) : paradata;
			result.setData(start, target);
			start++;
			target = result.getData(start).compare(paradata) <= 0 ?
					  result.getData(start) : paradata;
			result.setData(start, target);
			start += 2;
		}
	}
	
	/**
	 * This is the last step of setting the tuple: it will do the 
	 * division for the AVG aggregation and set the number of tuples
	 * to the COUNT operator.
	 * @param result the result tuple that will be modified.
	 * @param numoftuples the number of tuples used for COUNT.
	 */
	private void setlast(Tuple result, long numoftuples) {
		int start = groupindexlist.size() + 2;
		for(Map.Entry<String, Mule> entry : residuelist) {
			int type = entry.getValue().getIndex();
			if(type==1||type==5) {
				DataType temp = result.getData(start + 1);
				result.setData(start + 1, temp.divide(numoftuples));
				start += 2;
			}
			result.setData(start + 2, new DataType(numoftuples));
			start += 3;
		}
	}
	
}

