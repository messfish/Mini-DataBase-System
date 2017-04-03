package PhysicalOperators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Evaluator.Evaluator;
import SQLExpression.ColumnNode;
import SQLExpression.Expression;
import Support.Mule;
import TableElement.DataType;
import TableElement.Tuple;

/**
 * This class is mainly used to handle the projection part.
 * @author messfish
 *
 */
public class ProjectOperator extends Operator {

	private Operator operator;
	private List<Expression> list;
	private Map<String, Mule> schema;
	private Evaluator eva;
	private long tupleID; // this is used to trace the tuple ID.
	
	/**
	 * Constructor: this constructor is mainly used to pass the arguments
	 * to their global variables, respectively.
	 */
	public ProjectOperator(Operator op, List<Expression> list, 
			List<String> alias_list) {
		tupleID = 1;
		eva = new Evaluator(op.getSchema());
		operator = op;
		this.list = list;
		/* this indicates there is only an '*' for SELECT part. */
		if(list.size()==0)
			schema = op.getSchema();
		else {
			schema = new HashMap<>();
			for(int i=0;i<list.size();i++) {
				Expression express = list.get(i);
				if(alias_list.get(i).equals("")) {
					ColumnNode node = (ColumnNode)express;
					String str = node.getWholeColumnName();
					Mule mule = op.getSchema().get(str);
					mule.setIndex(i);
					schema.put(str, mule);
				}else {
					/* check whether there is only one elements in the tree. */
					Mule mule = null;
					if(express.isLeaf()) {
						ColumnNode node = (ColumnNode)express;
						String str = node.getWholeColumnName();
						mule = op.getSchema().get(str);
						mule.setIndex(i);
					}else {
						/* check whether there is a double type in the tree. */
						int datatype = 5;
						if(eva.noDouble(express))
							datatype = 1;
						mule = new Mule(i, datatype);
					}
					schema.put(alias_list.get(i), mule);
				}
			}
		}
	}

	/**
	 * This method is used to fetch the next tuple from the table.
	 * @return the next valid tuple.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple tuple = operator.getNextTuple();
		if(tuple == null) 
			return null;
		/* this indicates there is only an "*" in the SELECT query. */
		if(list.size()==0) {
			tuple.resetTupleID(tupleID);
			tupleID++;
			return tuple;
		}
		Tuple result = new Tuple(list.size(), 1);
		result.setTupleID(0, tupleID);
		for(int i=0;i<list.size();i++) {
			Evaluator eva=new Evaluator(tuple,list.get(i),operator.getSchema());
			DataType data = eva.getData();
			result.setData(i, data);
		}
		tupleID++;
		return result;
	}

	/**
	 * This method is used to reset the operator back to the starting point.
	 */
	@Override
	public void reset() {
		tupleID = 1;
		operator.reset();
	}

	/**
	 * This is the getter method of the schema of the operator.
	 * @return the schema of the operator.
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
		return 1;
	}
	
}
