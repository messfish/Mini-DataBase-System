package SQLExpression;

/**
 * For this class, it is mainly used to visit the expressions
 * that are abstract. It is used to deal with checking whether
 * two trees are equal since only using the abstract methods are
 * enough to traverse through the tree.
 * @author messfish
 *
 */
public interface AbstractVisitor {

	void visit(Expression express);
	
	void visit(BinaryExpression express);
	
	void visit(UnaryExpression express);
	
	void visit(MultipleExpression express);
	
}
