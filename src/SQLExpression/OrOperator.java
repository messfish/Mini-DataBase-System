package SQLExpression;

/**
 * this is the class for a binary expression to handle an OR logical operator.
 * @author messfish
 *
 */
public class OrOperator extends BinaryExpression{

	/**
	 * Constructor: this constructor extends from the binary expression
	 * and do what that expression does.
	 * @param left the left child.
	 * @param right the right child.
	 */
	public OrOperator(Expression left, Expression right){
		super(left, right);
	}

	/**
	 * this method is the method for accepting visitor: just calls the 
	 * visitor in the ExpressionVisitor Interface. The logic of the tree
	 * traverse will be handled by the class who implement that interface.
	 * @param expression the visitor to be accepted.
	 */
	@Override
	public void accept(ExpressionVisitor expression) {
		expression.visit(this);
	}
	
	/**
	 * this method is the method for accepting visitor: just calls the 
	 * visitor in the CloneExpressionVisitor Interface. The logic of the tree
	 * traverse will be handled by the class who implement that interface.
	 * @param expression the visitor to be accepted.
	 */
	@Override
	public Expression accept(CloneExpressionVisitor expression) {
		return expression.visit(this);
	}
	
	/**
	 * this is mainly used for debugging, it prints out a string to
	 * identify the type of the expression.
	 */
	@Override
	public String getString() {
		return "Or";
	}
	
	/**
	 * This is mainly used for debugging: it will print the 
	 * structure of the expression in a tree structure. The 
	 * number of "-" indicates the level of the tree.
	 * @param s a list of '-' to indicate the level of the tree.
	 * @param sb the string that will be used to generate the output.
	 */
	@Override
	public void print(String s, StringBuilder sb) {
		sb.append(s + "[OR]").append("\n");
		getLeftChild().print(s + "-", sb);
		getRightChild().print(s + "-", sb);
	}
	
	/**
	 * This is used to convert the tree structure into a string.
	 * perform the in order traversal of the tree. When we finish
	 * the traverse, the String will be stored in the string builder.
	 * @param sb the StringBuilder to store the target string.
	 */
	@Override
	public void toString(StringBuilder sb) {
		getLeftChild().toString(sb);
		sb.append("OR ");
		getRightChild().toString(sb);
	}

}
