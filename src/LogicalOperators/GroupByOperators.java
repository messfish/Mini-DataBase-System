package LogicalOperators;

/**
 * this class is the logical version of the group by operators.
 * @author messfish
 *
 */
public class GroupByOperators extends UnaryOperators{

	/**
	 * Constructor: this constructor extends the logic
	 * from its parent.
	 * @param child the children of this logical operator.
	 */
	public GroupByOperators(Operators child) {
		super(child);
	}

	/**
	 * this method just calls the visit method for the class
	 * that implements the operator visitor. The rest of the 
	 * logic will be handled by that class.
	 */
	@Override
	public void accept(OperatorVisitor operator) {
		operator.visit(this);
	}

	/**
	 * This method is mainly for debugging, it will store the 
	 * tree structure in the string builder.
	 * @param s the string that indicates the level of the tree.
	 * @param sb the string that stores the structure of the tree.
	 */
	@Override
	public void print(String s, StringBuilder sb) {
		sb.append(s).append("Group By:").append("\n");
		getChild().print(s+"-", sb);
	}
	
}
