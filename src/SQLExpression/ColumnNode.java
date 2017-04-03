package SQLExpression;

/**
 * This class handles the string value of the expression tree.
 * It extends the Unary expression class.
 * NOTE: THIS PART SHOULD BE MODIFIED!
 * @author messfish
 *
 */
public class ColumnNode extends Expression {

	private String data;
	
	/**
	 * Constructor: This class assign the data by using the argument.
	 * @param data the data that will be stored.
	 */
	public ColumnNode(String data) {
		this.data = data;
	}
	
	/**
	 * the getter method of the data.
	 * @return the data.
	 */
	public String getWholeColumnName() {
		return data;
	}
	
	/**
	 * this is the getter method of the table name from the column.
	 * @return the table name.
	 */
	public String getTableName() {
		return data.split(".")[0];
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
		return "Column: " + data;
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
		sb.append(s + "[" + "Column: " + data + "]").append("\n");
	}
	
	/**
	 * This is used to convert the tree structure into a string.
	 * perform the in order traversal of the tree. When we finish
	 * the traverse, the String will be stored in the string builder.
	 * @param sb the StringBuilder to store the target string.
	 */
	@Override
	public void toString(StringBuilder sb) {
		sb.append(data).append(" ");
	}

}
