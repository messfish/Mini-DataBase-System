package SQLExpression;

import SQLParser.PlainSelect;

/**
 * This is the class that deals with the nested query language.
 * @author messfish
 *
 */
public class Subselect extends Expression{

	private PlainSelect plain;
	// this variable stores a point to the plain select language.
	private String select;
	// this variable stores a select language.
	
	/**
	 * Constructor: this constructor takes a sub select query into
	 * a string as the paramter and build a plain select object.
	 * @param s
	 */
	public Subselect(String s) {
		select = s;
		plain = new PlainSelect(s);
	}
	
	/**
	 * the getter method of the plain select object.
	 * @return the plain select object.
	 */
	public PlainSelect getSelect() {
		return plain;
	}
	
	/**
	 * the getter method of the select query.
	 * @return the select query in string.
	 */
	@Override
	public String getString() {
		return select;
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
	 * This is mainly used for debugging: it will print the 
	 * structure of the expression in a tree structure. The 
	 * number of "-" indicates the level of the tree. For this
	 * time, I will use a "subquery" to indicate this is a 
	 * subquery.
	 * @param s a list of '-' to indicate the level of the tree.
	 * @param sb the string that will be used to generate the output.
	 */
	@Override
	public void print(String s, StringBuilder sb) {
		sb.append(s + "[Subquery]").append("\n");
	}

	/**
	 * This is used to convert the tree structure into a string.
	 * perform the in order traversal of the tree. When we finish
	 * the traverse, the String will be stored in the string builder.
	 * @param sb the StringBuilder to store the target string.
	 */
	@Override
	public void toString(StringBuilder sb) {
		sb.append(plain.getQuery());
	}

}
