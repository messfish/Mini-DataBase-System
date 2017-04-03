package PhysicalOperators;

import java.util.Map;

import Support.Mule;
import TableElement.DataType;
import TableElement.Tuple;

/**
 * This class is mainly used for handling the distinct operation.
 * Basically it will take the source from a sort operator. Doing 
 * this may let us leave out the duplicated tuple more efficiently.
 * 
 * @author messfish
 *
 */
public class DistinctOperator extends Operator {
	
	private Tuple tuple;
	private Operator op;
	
	/**
	 * Constructor: this constructor is used to build the operator based
	 * on the operator from the argument. Note in the argument, the tuples
	 * should be sorted.
	 * @param op the operator that will be used for passing data.
	 */
	public DistinctOperator(Operator op) {
		this.op = op;
		/* notice the distinct operator always deals the work after
		 * projection operator, so the number of tupleID should be 1. */
		tuple = new Tuple(op.getSchema().size(), 1);
		for(int i=0;i<tuple.datasize();i++)
			tuple.setData(i, new DataType(""));
	}

	/**
	 * This method is used to get the next valid tuple. get the next tuple
	 * from the operator until we find the tuple that does not match the 
	 * tuple in the global variable. Store that tuple into the global variable
	 * and return that tuple back.
	 * @return the next tuple available.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple temp = null;
		while(true) {
			temp = op.getNextTuple();
			if(!tuple.equals(temp))
				break;
		}
		tuple = temp;
		return temp;
	}

	/**
	 * This method is used to rest the operator back to the starting point.
	 */
	@Override
	public void reset() {
		op.reset();
		tuple = new Tuple(op.getSchema().size(), 1);
		for(int i=0;i<tuple.datasize();i++)
			tuple.setData(i, new DataType(""));
	}

	/**
	 * This is the getter method of the schema of the table.
	 * @return the schema of the table.
	 */
	@Override
	public Map<String, Mule> getSchema() {
		return op.getSchema();
	}
	
	/**
	 * This abstract method is used to fetch the number of tables in
	 * the single operator.
	 * @return the number of tables in this operator.
	 */
	@Override
	public int getNumOfTables() {
		return op.getNumOfTables();
	}

}
