package SQLExpression;

/**
 * this interface is mainly used for cloning. Therefore, as you
 * can see, every method in this interface has a return type.
 * @author messfish
 *
 */
public interface CloneExpressionVisitor {

	Expression visit(AddOperator operator);
	
	Expression visit(MinusOperator operator);
	
	Expression visit(MultiplyOperator operator);
	
	Expression visit(DivideOperator operator);
	
	Expression visit(NegativeValue operator);
	
	Expression visit(AndOperator operator);
	
	Expression visit(OrOperator operator);
	
	Expression visit(NotOperator operator);
	
	Expression visit(Equals comparator);
	
	Expression visit(NotEquals comparator);
	
	Expression visit(GreaterThan comparator);
	
	Expression visit(GreaterThanOrEquals comparator);
	
	Expression visit(LessThan comparator);
	
	Expression visit(LessThanOrEquals comparator);
	
	Expression visit(LikeOperator operator);
	
	Expression visit(Parenthesis expression);
	
	Expression visit(ExistsOperator operator);
	
	Expression visit(InOperator operator);
	
	Expression visit(AllOperator operator);
	
	Expression visit(AnyOperator operator);
	
	Expression visit(DoubleValue value);
	
	Expression visit(LongValue value);
	
	Expression visit(DateValue value);
	
	Expression visit(TimeValue value);
	
	Expression visit(StringValue value);
	
	Expression visit(ColumnNode node);
	
	Expression visit(Subselect subquery);
	
	Expression visit(MultiAndOperator operator);
	
	Expression visit(MultiOrOperator operator);

}
