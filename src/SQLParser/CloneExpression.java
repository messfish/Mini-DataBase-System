package SQLParser;

import java.util.ArrayList;
import java.util.List;

import SQLExpression.AddOperator;
import SQLExpression.AllOperator;
import SQLExpression.AndOperator;
import SQLExpression.AnyOperator;
import SQLExpression.CloneExpressionVisitor;
import SQLExpression.ColumnNode;
import SQLExpression.DateValue;
import SQLExpression.DivideOperator;
import SQLExpression.DoubleValue;
import SQLExpression.Equals;
import SQLExpression.ExistsOperator;
import SQLExpression.Expression;
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

/**
 * This class is used to clone an expression tree with some specific rules:
 * All the parenthesis in this tree will be left out. All the "and" and "or"
 * operators will be turned into multi-and and multi-or operators. Others 
 * remain the same.
 * @author messfish
 *
 */
public class CloneExpression implements CloneExpressionVisitor {

	/**
	 * this method makes a clone of the expression tree and return
	 * the root of the newly cloned one.
	 * @param express the root of the expression tree.
	 * @return the newly cloned root of the expression tree.
	 */
	public Expression clone(Expression express) {
		return express.accept(this);
	}
	
	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(AddOperator operator) {
		Expression left = operator.getLeftChild().accept(this);
		Expression right = operator.getRightChild().accept(this);
		return new AddOperator(left,right);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(MinusOperator operator) {
		Expression left = operator.getLeftChild().accept(this);
		Expression right = operator.getRightChild().accept(this);
		return new MinusOperator(left, right);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(MultiplyOperator operator) {
		Expression left = operator.getLeftChild().accept(this);
		Expression right = operator.getRightChild().accept(this);
		return new MultiplyOperator(left, right);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(DivideOperator operator) {
		Expression left = operator.getLeftChild().accept(this);
		Expression right = operator.getRightChild().accept(this);
		return new DivideOperator(left, right);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * which contains the child.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(NegativeValue operator) {
		Expression child = operator.getChild().accept(this);
		return new NegativeValue(child);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children. Since this is an and operator, we need
	 * to transform it into the "multi and" operator as specified in
	 * the definition of the class above.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(AndOperator operator) {
		Expression left = operator.getLeftChild().accept(this);
		Expression right = operator.getRightChild().accept(this);
		List<Expression> list = new ArrayList<>();
		list.add(left);
		list.add(right);
		return new MultiAndOperator(list);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children. Since this is an or operator, we need to
	 * transform it into the "multi or" operator as specified in the 
	 * definition of the class above.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(OrOperator operator) {
		Expression left = operator.getLeftChild().accept(this);
		Expression right = operator.getRightChild().accept(this);
		List<Expression> list = new ArrayList<>();
		list.add(left);
		list.add(right);
		return new MultiOrOperator(list);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * that contains the child.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(NotOperator operator) {
		Expression child = operator.getChild().accept(this);
		return new NotOperator(child);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(Equals comparator) {
		Expression left = comparator.getLeftChild().accept(this);
		Expression right = comparator.getRightChild().accept(this);
		return new Equals(left, right);
	}
	
	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(NotEquals comparator) {
		Expression left = comparator.getLeftChild().accept(this);
		Expression right = comparator.getRightChild().accept(this);
		return new NotEquals(left, right);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(GreaterThan comparator) {
		Expression left = comparator.getLeftChild().accept(this);
		Expression right = comparator.getRightChild().accept(this);
		return new GreaterThan(left, right);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(GreaterThanOrEquals comparator) {
		Expression left = comparator.getLeftChild().accept(this);
		Expression right = comparator.getRightChild().accept(this);
		return new GreaterThanOrEquals(left, right);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(LessThan comparator) {
		Expression left = comparator.getLeftChild().accept(this);
		Expression right = comparator.getRightChild().accept(this);
		return new LessThan(left, right);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(LessThanOrEquals comparator) {
		Expression left = comparator.getLeftChild().accept(this);
		Expression right = comparator.getRightChild().accept(this);
		return new LessThanOrEquals(left, right);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(LikeOperator operator) {
		Expression left = operator.getLeftChild().accept(this);
		Expression right = operator.getRightChild().accept(this);
		return new LikeOperator(left, right);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * since this is a parenthesis expression, there is no need to include
	 * that in the newly created tree, as specified in the class definition.
	 * Therefore, simply return the result.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(Parenthesis expression) {
		return expression.getChild().accept(this);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * that contains the children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(ExistsOperator operator) {
		Expression child = operator.getChild().accept(this);
		return new ExistsOperator(child);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * to combine two children.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(InOperator operator) {
		Expression left = operator.getLeftChild().accept(this);
		Expression right = operator.getRightChild().accept(this);
		return new InOperator(left, right);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * that contains the child.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(AllOperator operator) {
		Expression child = operator.getChild().accept(this);
		return new AllOperator(child);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * get the children from the subtree and generate a new expression
	 * that contains the child.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(AnyOperator operator) {
		Expression child = operator.getChild().accept(this);
		return new AnyOperator(child);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * generate a new double expression with the copied value.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(DoubleValue value) {
		return new DoubleValue(value.getData());
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * generate a new string expression with the copied value.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(StringValue value) {
		return new StringValue(value.getData());
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * generate a new column expression with the copied value.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(ColumnNode node) {
		return new ColumnNode(node.getWholeColumnName());
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * generate a new subselect expression with the copied value.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(Subselect subquery) {
		return new Subselect(subquery.getString());
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * generate a new list with all the children copied, store the list
	 * in the new multi and operator.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(MultiAndOperator operator) {
		List<Expression> list = new ArrayList<>();
		for(int i=0;i<operator.size();i++)
			list.add(operator.getChild(i).accept(this));
		return new MultiAndOperator(list);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * generate a new list with all the children copied, store the list
	 * in the new multi and operator.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(MultiOrOperator operator) {
		List<Expression> list = new ArrayList<>();
		for(int i=0;i<operator.size();i++)
			list.add(operator.getChild(i).accept(this));
		return new MultiOrOperator(list);
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * generate a new long expression with the copied value.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(LongValue value) {
		return new LongValue(value.getData());
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * generate a new data expression with the copied value.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(DateValue value) {
		return new DateValue(value.getData());
	}

	/**
	 * this method makes a clone of the expression the method specifies.
	 * generate a new data expression with the copied value.
	 * @param operator the expression that will be visited.
	 * @return the newly cloned expression.
	 */
	@Override
	public Expression visit(TimeValue value) {
		return new TimeValue(value.getData());
	}

}
