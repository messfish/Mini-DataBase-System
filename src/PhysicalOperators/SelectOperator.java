package PhysicalOperators;

import java.util.Map;

import Evaluator.Evaluator;
import SQLExpression.Expression;
import Support.Mule;
import TableElement.Tuple;

/**
 * This class handles the logic of scan operator. Basically it scans the 
 * whole table(which usually indicates we need a scan operator). After that
 * , I use a class that implements the expression visitor to tell whether
 * this tuple is a valid one. Scan the next tuple until we get the valid
 * one and return that tuple.
 * @author messfish
 *
 */
public class SelectOperator extends Operator{

	private Operator scan; // object that performs the scanning.
	private Expression express; // object stores the expression.
	
	/**
	 * Constructor: this constructor is used to set the expression to the
	 * global variable and pass the operator.
	 * @param operator the operator needs to be passed.
	 * @param express the expression that will be passed.
	 */
	public SelectOperator(Operator operator, Expression express) {
		scan = operator;
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
		Tuple tuple = scan.getNextTuple();
		if(tuple == null) return null;
		/* this usually indicates no where language. return that tuple. */
		if(express==null) return tuple;
		Evaluator eva = new Evaluator(tuple, express, getSchema());
		while(!eva.checkValid()) {
			tuple = scan.getNextTuple();
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
		scan.reset();
	}

	/**
	 * This method is used to get the schema of the table. Basically
	 * it just returns the schema from the Operator.
	 * @return the schema of the table.
	 */
	@Override
	public Map<String, Mule> getSchema() {
		return scan.getSchema();
	}
	
	/**
	 * This abstract method is used to fetch the number of tables in
	 * the single operator.
	 * @return the number of tables in this operator.
	 */
	@Override
	public int getNumOfTables() {
		return scan.getNumOfTables();
	}
	
}
