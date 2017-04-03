package PhysicalOperators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import Evaluator.Evaluator;
import SQLExpression.Expression;
import Support.Mule;
import TableElement.DataType;
import TableElement.Tuple;

/** 
 * This class is used to handle the "Order by" language, note this
 * class mainly performs an in memory sort. So it is only used to deal
 * with the case when the file sorted does not exceed 100MB. 
 * @author messfish
 *
 */
public class SortOperator extends Operator {
	
	private List<Tuple> tuplelist;
	private Map<String, Mule> schema;
	private int index; // the current index of the tuple list.
	private int numoftables;
	
	/**
	 * Constructor: this constructor gets all the tuples from an operator
	 * and stores then in a list of tuples. Next we will sort the tuples by
	 * using the orders from the order by list. For the attributes who does
	 * not show up in the order by list. Order them by using the appearance
	 * of the tuple list. Doing this will be handy for debugging. 
	 * @param op the operator that used for getting source.
	 * @param orderlist the list of expression for sorting.
	 * @param desclist the array checks whether the expression is ascending
	 * or descending by showing whether it is 1 or -1.
	 */
	public SortOperator(Operator op, List<Expression> orderlist, int[] desclist) {
		numoftables = op.getNumOfTables();
		tuplelist = new ArrayList<>();
		Tuple tuple = null;
		schema = op.getSchema();
		while((tuple=op.getNextTuple())!=null)
			tuplelist.add(tuple);
		Collections.sort(tuplelist, new Comparator<Tuple>(){
			@Override
			public int compare(Tuple tuple1, Tuple tuple2) {
				for(int i=0;i<orderlist.size();i++) {
					Expression exp = orderlist.get(i);
					Evaluator eva1 = new Evaluator(tuple1, exp, schema);
					DataType data1 = eva1.getData();
					Evaluator eva2 = new Evaluator(tuple2, exp, schema);
					DataType data2 = eva2.getData();
					int result = data1.compare(data2);
					if(result!=0) 
						return result * desclist[i];
				}
				for(int i=0;i<tuple1.datasize();i++) {
					DataType data1 = tuple1.getData(i);
					DataType data2 = tuple2.getData(i);
					int result = data1.compare(data2);
					if(result!=0) return result;
				}
				/* this means the two tuples are equal, return 0. */
				return 0;
			}
		});
	}

	/**
	 * This method is used to fetch the next tuple available.
	 * @return the next tuple.
	 */
	@Override
	public Tuple getNextTuple() {
		if(index == tuplelist.size())
			return null;
		Tuple tuple = tuplelist.get(index);
		index++;
		return tuple;
	}

	/**
	 * This method is used to reset the tuple back to the starting point.
	 * for this operator, the index will be set to zero.
	 */
	@Override
	public void reset() {
		index = 0;
	}

	/**
	 * This is the getter method of the schema of the operator.
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

}
