package Evaluator;

import java.util.Map;
import java.util.Stack;

import SQLExpression.AddOperator;
import SQLExpression.AllOperator;
import SQLExpression.AndOperator;
import SQLExpression.AnyOperator;
import SQLExpression.ColumnNode;
import SQLExpression.DateValue;
import SQLExpression.DivideOperator;
import SQLExpression.DoubleValue;
import SQLExpression.Equals;
import SQLExpression.ExistsOperator;
import SQLExpression.Expression;
import SQLExpression.ExpressionVisitor;
import SQLExpression.GreaterThan;
import SQLExpression.GreaterThanOrEquals;
import SQLExpression.InOperator;
import SQLExpression.LessThan;
import SQLExpression.LessThanOrEquals;
import SQLExpression.LikeOperator;
import SQLExpression.LongValue;
import SQLExpression.MinusOperator;
import SQLExpression.MultiAndOperator;
import SQLExpression.MultiOrOperator;
import SQLExpression.MultiplyOperator;
import SQLExpression.NegativeValue;
import SQLExpression.NotEquals;
import SQLExpression.NotOperator;
import SQLExpression.OrOperator;
import SQLExpression.Parenthesis;
import SQLExpression.StringValue;
import SQLExpression.Subselect;
import SQLExpression.TimeValue;
import TableElement.DataType;
import TableElement.Tuple;
import Support.Mule;
import Support.TimeConversion;

/**
 * This class is used to evaluate whether the tuple in the constructor
 * is valid or not. Basically it will return a boolean value. It follows
 * this logic: build two stacks. One is for handling different data types,
 * one is storing the boolean values. 
 * @author messfish
 *
 */
public class Evaluator implements ExpressionVisitor {

	private Tuple tuple;
	private Expression express;
	private Map<String, Mule> schema;
	private Stack<DataType> stack1;
	private Stack<Boolean> stack2;
	private boolean noDouble; 
	// the variable checks whether there is a double value in the expression.
	TimeConversion convert = new TimeConversion();
	
	/**
	 * Constructor: this constructor is used to pass the tuple and
	 * the expression to their global variable.
	 * @param tuple the tuple that will be checked.
	 * @param express the expression that evaluates the tuple.
	 * @param schema the schema that the table possess.
	 */
	public Evaluator(Tuple tuple, Expression express, Map<String, Mule> schema) {
		this.tuple = tuple;
		this.express = express;
		this.schema = schema;
		stack1 = new Stack<>();
		stack2 = new Stack<>();
	}
	
	/**
	 * Constructor: this constructor is used to pass the tuple and
	 * the expression to their global variable. Notice this is mainly
	 * used by the Operators who do not need to perform an evaluation.
	 * @param schema the schema that the table possess.
	 */
	public Evaluator(Map<String, Mule> schema) {
		/* this fake tuple is used for traversing through the expression
		 * tree since the column node will need to extract data. */
		tuple = new Tuple(schema.size(), 1);
		this.schema = schema;
		stack1 = new Stack<>();
		stack2 = new Stack<>();
		noDouble = true;
	}
	
	/**
	 * This method checks whether the tuple is a valid one. Post traverse the 
	 * expression tree and manipulate the two stacks. After the traverse,
	 * there could be one boolean value left on the stack. That value indicates
	 * whether the tuple is valid or not. return that value from the stack.
	 * @return the boolean value shows whether the tuple is valid or not.
	 */
	public boolean checkValid() {
		express.accept(this);
		/* this could only happen when the expression is empty. In this case,
		 * simply return a true value. */
		if(stack2.isEmpty())
			return true;
		return stack2.pop();
	}
	
	/**
	 * This method is used to get the data after the calculation. Post traverse
	 * the expression tree and manipulate the first stack only. After the 
	 * traverse, there could be one boolean value left on the stack. That value
	 * indicates the result of the data. Return that data from the stack.
	 * @return the result of the calculation from expression.
	 */
	public DataType getData() {
		express.accept(this);
		return stack1.pop();
	}
	
	/**
	 * this method returns a value evaluates the statement " there are no
	 * double value type in the expression tree". Traverse the tree and 
	 * return the modified(if it is) noDouble.
	 * @param express the expression tree that will be checked.
	 * @return a value indicates whether a double value type exists.
	 */
	public boolean noDouble(Expression express) {
		express.accept(this);
		return noDouble;
	}
	
	/**
	 * This method handles the addition of two data types. It pops out
	 * two data type from stack1. Doing an addition and push it back on
	 * stack1.
	 * @param operator operator that handles the addition.
	 */
	@Override
	public void visit(AddOperator operator) {
		operator.getLeftChild().accept(this);
		operator.getRightChild().accept(this);
		DataType data1 = stack1.pop(), data2 = stack1.pop();
		/* normally there could only be four data types. enumerate them
		 * all and perform the calculation separately. */
		if(data1.getType()==1&&data2.getType()==1) {
			long number1 = data1.getLong(), number2 = data2.getLong();
			stack1.push(new DataType(number2 + number1));
		}else if(data1.getType()==1&&data2.getType()==5) {
			long number1 = data1.getLong();
			double number2 = data2.getDouble();
			stack1.push(new DataType(number2 + number1));
		}else if(data1.getType()==5&&data2.getType()==1) {
			double number1 = data1.getDouble();
			long number2 = data2.getLong();
			stack1.push(new DataType(number2 + number1));
		}else {
			double number1 = data1.getDouble(), number2 = data2.getDouble();
			stack1.push(new DataType(number2 + number1));
		}
	}

	/**
	 * This method handles the subtraction of two data types. It pops out
	 * two data type from stack1. Doing a subtraction and push it back on
	 * stack1.
	 * @param operator operator that handles the addition.
	 */
	@Override
	public void visit(MinusOperator operator) {
		operator.getLeftChild().accept(this);
		operator.getRightChild().accept(this);
		DataType data1 = stack1.pop(), data2 = stack1.pop();
		/* normally there could only be four data types. enumerate them
		 * all and perform the calculation separately. */
		if(data1.getType()==1&&data2.getType()==1) {
			long number1 = data1.getLong(), number2 = data2.getLong();
			stack1.push(new DataType(number2 - number1));
		}else if(data1.getType()==1&&data2.getType()==5) {
			long number1 = data1.getLong();
			double number2 = data2.getDouble();
			stack1.push(new DataType(number2 - number1));
		}else if(data1.getType()==5&&data2.getType()==1) {
			double number1 = data1.getDouble();
			long number2 = data2.getLong();
			stack1.push(new DataType(number2 - number1));
		}else {
			double number1 = data1.getDouble(), number2 = data2.getDouble();
			stack1.push(new DataType(number2 - number1));
		}
	}

	/**
	 * This method handles the multiplication of two data types. It pops out
	 * two data type from stack1. Doing a multiplication and push it back on
	 * stack1.
	 * @param operator operator that handles the multiplication.
	 */
	@Override
	public void visit(MultiplyOperator operator) {
		operator.getLeftChild().accept(this);
		operator.getRightChild().accept(this);
		DataType data1 = stack1.pop(), data2 = stack1.pop();
		/* normally there could only be four data type combinations.
		 * enumerate them all and perform the calculation separately. */
		if(data1.getType()==1&&data2.getType()==1) {
			long number1 = data1.getLong(), number2 = data2.getLong();
			stack1.push(new DataType(number2 * number1));
		}else if(data1.getType()==1&&data2.getType()==5) {
			long number1 = data1.getLong();
			double number2 = data2.getDouble();
			stack1.push(new DataType(number2 * number1));
		}else if(data1.getType()==5&&data2.getType()==1) {
			double number1 = data1.getDouble();
			long number2 = data2.getLong();
			stack1.push(new DataType(number2 * number1));
		}else {
			double number1 = data1.getDouble(), number2 = data2.getDouble();
			stack1.push(new DataType(number2 * number1));
		}
	}

	/**
	 * This method handles the division of two data types. It pops out
	 * two data type from stack1. Doing a division and push it back on
	 * stack1.
	 * @param operator operator that handles the division.
	 */
	@Override
	public void visit(DivideOperator operator) {
		operator.getLeftChild().accept(this);
		operator.getRightChild().accept(this);
		DataType data1 = stack1.pop(), data2 = stack1.pop();
		/* normally there could only be four data type combinations.
		 * enumerate them all and perform the calculation separately. */
		if(data1.getType()==1&&data2.getType()==1) {
			long number1 = data1.getLong(), number2 = data2.getLong();
			stack1.push(new DataType(number2 / number1));
		}else if(data1.getType()==1&&data2.getType()==5) {
			long number1 = data1.getLong();
			double number2 = data2.getDouble();
			stack1.push(new DataType(number2 / number1));
		}else if(data1.getType()==5&&data2.getType()==1) {
			double number1 = data1.getDouble();
			long number2 = data2.getLong();
			stack1.push(new DataType(number2 / number1));
		}else {
			double number1 = data1.getDouble(), number2 = data2.getDouble();
			stack1.push(new DataType(number2 / number1));
		}
	}

	/**
	 * This method handles the negative expression. It pops out a datatype
	 * of the stack1. set the value to negative and restore back. 
	 * @param operator the operator that handles the negative operation.
	 */
	@Override
	public void visit(NegativeValue operator) {
		operator.getChild().accept(this);
		DataType data = stack1.pop();
		/* the type of the data could only be a long integer or a double
		 * value, handle them separately. */
		if(data.getType()==1) {
			long number = data.getLong();
			stack1.push(new DataType(-number));
		}else {
			double number = data.getDouble();
			stack1.push(new DataType(-number));
		}
	}

	/**
	 * This method handles the and operator. Pop two boolean values from
	 * stack2 and perform an and operation. push the result back. 
	 * @param operator the and operator that needs to be handled.
	 */
	@Override
	public void visit(AndOperator operator) {
		operator.getLeftChild().accept(this);
		operator.getRightChild().accept(this);
		boolean right = stack2.pop(), left = stack2.pop();
		stack2.push(left & right);
	}

	/**
	 * This method handles the or operator. Pop two boolean values from
	 * stack2 and perform an or operation. push the result back. 
	 * @param operator the or operator that needs to be handled.
	 */
	@Override
	public void visit(OrOperator operator) {
		operator.getLeftChild().accept(this);
		operator.getRightChild().accept(this);
		boolean right = stack2.pop(), left = stack2.pop();
		stack2.push(left | right);
	}

	/**
	 * This method handles the or operator. Pop one boolean value from
	 * stack2 and perform a not operation. push the result back. 
	 * @param operator the not operator that needs to be handled.
	 */
	@Override
	public void visit(NotOperator operator) {
		operator.getChild().accept(this);
		boolean child = stack2.pop();
		stack2.push(!child);
	}

	/**
	 * This method handles the equals operator. Pop two data type value
	 * from stack1 and perform an equal operation. push the boolean result
	 * into stack2.
	 * @param comparator the comparator performs the comparison.
	 */
	@Override
	public void visit(Equals comparator) {
		comparator.getLeftChild().accept(this);
		comparator.getRightChild().accept(this);
		DataType data1 = stack1.pop(), data2 = stack1.pop();
		/* normally there could only be five data type combinations.
		 * enumerate them all and perform the calculation separately. */
		if(data1.getType()==1&&data2.getType()==1) {
			long number1 = data1.getLong(), number2 = data2.getLong();
			stack2.push(number2 == number1);
		}else if(data1.getType()==1&&data2.getType()==5) {
			long number1 = data1.getLong();
			double number2 = data2.getDouble();
			stack2.push(number2 == number1);
		}else if(data1.getType()==5&&data2.getType()==1) {
			double number1 = data1.getDouble();
			long number2 = data2.getLong();
			stack2.push(number2 == number1);
		}else if(data1.getType()==5&&data2.getType()==5) {
			double number1 = data1.getDouble(), number2 = data2.getDouble();
			stack2.push(number2 == number1);
		}else if(data1.getType()==2&&data2.getType()==2) {
			String str1 = data1.getString(), str2 = data2.getString();
			stack2.push(str2.compareTo(str1) == 0);
		}
	}

	/**
	 * This method handles the not equal operator. Pop two data type value
	 * from stack1 and perform a not equal operation. push the boolean result
	 * into stack2.
	 * @param comparator the comparator performs the comparison.
	 */
	@Override
	public void visit(NotEquals comparator) {
		comparator.getLeftChild().accept(this);
		comparator.getRightChild().accept(this);
		DataType data1 = stack1.pop(), data2 = stack1.pop();
		/* normally there could only be five data type combinations.
		 * enumerate them all and perform the calculation separately. */
		if(data1.getType()==1&&data2.getType()==1) {
			long number1 = data1.getLong(), number2 = data2.getLong();
			stack2.push(number2 != number1);
		}else if(data1.getType()==1&&data2.getType()==5) {
			long number1 = data1.getLong();
			double number2 = data2.getDouble();
			stack2.push(number2 != number1);
		}else if(data1.getType()==5&&data2.getType()==1) {
			double number1 = data1.getDouble();
			long number2 = data2.getLong();
			stack2.push(number2 != number1);
		}else if(data1.getType()==5&&data2.getType()==5) {
			double number1 = data1.getDouble(), number2 = data2.getDouble();
			stack2.push(number2 != number1);
		}else if(data1.getType()==2&&data2.getType()==2) {
			String str1 = data1.getString(), str2 = data2.getString();
			stack2.push(str2.compareTo(str1) != 0);
		}
	}

	/**
	 * This method handles the greater than operator. Pop two data type value
	 * from stack1 and perform a greater than operation. push the boolean result
	 * into stack2.
	 * @param comparator the comparator performs the comparison.
	 */
	@Override
	public void visit(GreaterThan comparator) {
		comparator.getLeftChild().accept(this);
		comparator.getRightChild().accept(this);
		DataType data1 = stack1.pop(), data2 = stack1.pop();
		/* normally there could only be five data type combinations.
		 * enumerate them all and perform the calculation separately. */
		if(data1.getType()==1&&data2.getType()==1) {
			long number1 = data1.getLong(), number2 = data2.getLong();
			stack2.push(number2 > number1);
		}else if(data1.getType()==1&&data2.getType()==5) {
			long number1 = data1.getLong();
			double number2 = data2.getDouble();
			stack2.push(number2 > number1);
		}else if(data1.getType()==5&&data2.getType()==1) {
			double number1 = data1.getDouble();
			long number2 = data2.getLong();
			stack2.push(number2 > number1);
		}else if(data1.getType()==5&&data2.getType()==5) {
			double number1 = data1.getDouble(), number2 = data2.getDouble();
			stack2.push(number2 > number1);
		}else if(data1.getType()==2&&data2.getType()==2) {
			String str1 = data1.getString(), str2 = data2.getString();
			stack2.push(str2.compareTo(str1) > 0);
		}
	}

	/**
	 * This method handles the greater than or equal operator. 
	 * Pop two data type value from stack1 and perform a 
	 * greater than or equal operation. push the boolean result into stack2.
	 * @param comparator the comparator performs the comparison.
	 */
	@Override
	public void visit(GreaterThanOrEquals comparator) {
		comparator.getLeftChild().accept(this);
		comparator.getRightChild().accept(this);
		DataType data1 = stack1.pop(), data2 = stack1.pop();
		/* normally there could only be five data type combinations.
		 * enumerate them all and perform the calculation separately. */
		if(data1.getType()==1&&data2.getType()==1) {
			long number1 = data1.getLong(), number2 = data2.getLong();
			stack2.push(number2 >= number1);
		}else if(data1.getType()==1&&data2.getType()==5) {
			long number1 = data1.getLong();
			double number2 = data2.getDouble();
			stack2.push(number2 >= number1);
		}else if(data1.getType()==5&&data2.getType()==1) {
			double number1 = data1.getDouble();
			long number2 = data2.getLong();
			stack2.push(number2 >= number1);
		}else if(data1.getType()==5&&data2.getType()==5) {
			double number1 = data1.getDouble(), number2 = data2.getDouble();
			stack2.push(number2 >= number1);
		}else if(data1.getType()==2&&data2.getType()==2) {
			String str1 = data1.getString(), str2 = data2.getString();
			stack2.push(str2.compareTo(str1) >= 0);
		}
	}

	/**
	 * This method handles the less than operator. Pop two data type value
	 * from stack1 and perform a less than operation. push the boolean result
	 * into stack2.
	 * @param comparator the comparator performs the comparison.
	 */
	@Override
	public void visit(LessThan comparator) {
		comparator.getLeftChild().accept(this);
		comparator.getRightChild().accept(this);
		DataType data1 = stack1.pop(), data2 = stack1.pop();
		/* normally there could only be five data type combinations.
		 * enumerate them all and perform the calculation separately. */
		if(data1.getType()==1&&data2.getType()==1) {
			long number1 = data1.getLong(), number2 = data2.getLong();
			stack2.push(number2 < number1);
		}else if(data1.getType()==1&&data2.getType()==5) {
			long number1 = data1.getLong();
			double number2 = data2.getDouble();
			stack2.push(number2 < number1);
		}else if(data1.getType()==5&&data2.getType()==1) {
			double number1 = data1.getDouble();
			long number2 = data2.getLong();
			stack2.push(number2 < number1);
		}else if(data1.getType()==5&&data2.getType()==5) {
			double number1 = data1.getDouble(), number2 = data2.getDouble();
			stack2.push(number2 < number1);
		}else if(data1.getType()==2&&data2.getType()==2) {
			String str1 = data1.getString(), str2 = data2.getString();
			stack2.push(str2.compareTo(str1) < 0);
		}
	}

	/**
	 * This method handles the less than or equal operator. 
	 * Pop two data type value from stack1 and perform a 
	 * less than or equal operation. push the boolean result into stack2.
	 * @param comparator the comparator performs the comparison.
	 */
	@Override
	public void visit(LessThanOrEquals comparator) {
		comparator.getLeftChild().accept(this);
		comparator.getRightChild().accept(this);
		DataType data1 = stack1.pop(), data2 = stack1.pop();
		/* normally there could only be five data type combinations.
		 * enumerate them all and perform the calculation separately. */
		if(data1.getType()==1&&data2.getType()==1) {
			long number1 = data1.getLong(), number2 = data2.getLong();
			stack2.push(number2 <= number1);
		}else if(data1.getType()==1&&data2.getType()==5) {
			long number1 = data1.getLong();
			double number2 = data2.getDouble();
			stack2.push(number2 <= number1);
		}else if(data1.getType()==5&&data2.getType()==1) {
			double number1 = data1.getDouble();
			long number2 = data2.getLong();
			stack2.push(number2 <= number1);
		}else if(data1.getType()==5&&data2.getType()==5) {
			double number1 = data1.getDouble(), number2 = data2.getDouble();
			stack2.push(number2 <= number1);
		}else if(data1.getType()==2&&data2.getType()==2) {
			String str1 = data1.getString(), str2 = data2.getString();
			stack2.push(str2.compareTo(str1) <= 0);
		}
	}

	@Override
	public void visit(LikeOperator operator) {
		
	}

	/**
	 * This method handles the parenthesis operator. just call the
	 * children inside the parenthesis.
	 * @param expression the parenthesis that is visited.
	 */
	@Override
	public void visit(Parenthesis expression) {
		expression.getChild().accept(this);
	}

	@Override
	public void visit(ExistsOperator operator) {
		
	}

	@Override
	public void visit(InOperator operator) {
		
	}

	@Override
	public void visit(AllOperator operator) {
		
	}

	@Override
	public void visit(AnyOperator operator) {
		
	}

	/**
	 * This method is used to deal with the double value in the expression.
	 * store that value into a data type and push that into stack1.
	 * Also, change the noDouble variable to false;
	 * @param value the double value in an expression.
	 */
	@Override
	public void visit(DoubleValue value) {
		noDouble = false;
		stack1.push(new DataType(value.getData()));
	}

	/**
	 * This method is used to deal with the string value in the expression.
	 * store that value into a data type and push that into stack1.
	 * @param value the string value in an expression.
	 */
	@Override
	public void visit(StringValue value) {
		stack1.push(new DataType(value.getData()));
	}

	/**
	 * This method is used to deal with the column node in the expression.
	 * check the index of that column from the schema hash map. fetch the
	 * data type by using the index and store that into stack1. Also, if 
	 * the type of the data is double, set noDouble to false.
	 * @param value the column node that will be visited.
	 */
	@Override
	public void visit(ColumnNode node) {
		String attribute = node.getWholeColumnName();
		Mule mule = schema.get(attribute);
		DataType data = tuple.getData(mule.getIndex());
		if(mule.getDataType()==5)
			noDouble = false;
		stack1.push(data);
	}

	@Override
	public void visit(Subselect subquery) {
		
	}

	@Override
	public void visit(MultiAndOperator operator) {
		
	}

	@Override
	public void visit(MultiOrOperator operator) {
		
	}

	/**
	 * This method is used to deal with the long value in the expression.
	 * store that value into a data type and push that into stack1.
	 * @param value the long value in an expression.
	 */
	@Override
	public void visit(LongValue value) {
		stack1.push(new DataType(value.getData()));
	}

	/**
	 * This method is used to deal with the date value in the expression.
	 * convert the date value type into a double value and
	 * store that value into a data type and push that into stack1.
	 * @param value the date value in an expression.
	 */
	@Override
	public void visit(DateValue value) {
		double data = convert.fromDateToNumber(value.getData());
		stack1.push(new DataType(data));
	}

	/**
	 * This method is used to deal with the time value in the expression.
	 * convert the time value type into a double value and
	 * store that value into a data type and push that into stack1.
	 * @param value the time value in an expression.
	 */
	@Override
	public void visit(TimeValue value) {
		double data = convert.fromTimeToNumber(value.getData());
		stack1.push(new DataType(data));
	}

}
