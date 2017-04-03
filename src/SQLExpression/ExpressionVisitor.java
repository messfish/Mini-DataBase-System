package SQLExpression;

/**
 * This interface stores the visit of all concrete expressions.
 * @author messfish
 *
 */
public interface ExpressionVisitor {

	void visit(AddOperator operator);
	
	void visit(MinusOperator operator);
	
	void visit(MultiplyOperator operator);
	
	void visit(DivideOperator operator);
	
	void visit(NegativeValue operator);
	
	void visit(AndOperator operator);
	
	void visit(OrOperator operator);
	
	void visit(NotOperator operator);
	
	void visit(Equals comparator);
	
	void visit(NotEquals comparator);
	
	void visit(GreaterThan comparator);
	
	void visit(GreaterThanOrEquals comparator);
	
	void visit(LessThan comparator);
	
	void visit(LessThanOrEquals comparator);
	
	void visit(LikeOperator operator);
	
	void visit(Parenthesis expression);
	
	void visit(ExistsOperator operator);
	
	void visit(InOperator operator);
	
	void visit(AllOperator operator);
	
	void visit(AnyOperator operator);
	
	void visit(DoubleValue value);
	
	void visit(LongValue value);
	
	void visit(DateValue value);
	
	void visit(TimeValue value);
	
	void visit(StringValue value);
	
	void visit(ColumnNode node);
	
	void visit(Subselect subquery);
	
	void visit(MultiAndOperator operator);
	
	void visit(MultiOrOperator operator);
	
}
