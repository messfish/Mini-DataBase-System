package PhysicalOperators;

import java.util.Map;

import Evaluator.Evaluator;
import SQLExpression.Expression;
import Support.Mule;
import TableElement.Tuple;

/**
 * This class is mainly used to handle the having expression.
 * It is exactly similar to the where expression: evaluate the 
 * tuples coming from the GroupByOperator and return that tuple if that
 * tuple satisfies the evaluation.
 * @author messfish
 *
 */
public class HavingOperator extends Operator {

	private Operator op; // object that performs the scanning.
	private Expression express; // object stores the expression.
	
	/**
	 * Constructor: this constructor is used to set the expression to the
	 * global variable and pass the operator.
	 * @param operator the operator needs to be passed.
	 * @param express the expression that will be passed.
	 */
	public HavingOperator(Operator operator, Expression express) {
		op = operator;
		this.express = express;
	}

	/**
	 * This method is used to get the next valid tuple from the 
	 * operator. get one tuple from the Operator, use a evaluator
	 * to check whether it is valid or not. If not, get the next tuple
	 * until the tuple is valid. If yes, return that tuple.
	 * @return the tuple that pass the evaluation.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple tuple = op.getNextTuple();
		if(tuple == null) return null;
		/* this usually indicates no where language. return that tuple. */
		if(express==null) return tuple;
		Evaluator eva = new Evaluator(tuple, express, getSchema());
		while(!eva.checkValid()) {
			tuple = op.getNextTuple();
			if(tuple == null) return null;
			eva = new Evaluator(tuple,express,getSchema());
		}
		return tuple;
	}

	/**
	 * This method is mainly used for reseting the pointer back to 
	 * the starting point of the table. Just simply call the reset() 
	 * method from the Operator.
	 */
	@Override
	public void reset() {
		op.reset();
	}

	/**
	 * This method is used to get the schema of the table. Basically
	 * it just returns the schema from the Operator.
	 * @return the schema of the table.
	 */
	@Override
	public Map<String, Mule> getSchema() {
		return op.getSchema();
	}
	
	/**
	 * This abstract method is used to fetch the number of tables in
	 * the single operator. For this operator, we simply return 1.
	 * @return the number of tables in this operator.
	 */
	@Override
	public int getNumOfTables() {
		return 1;
	}

}
