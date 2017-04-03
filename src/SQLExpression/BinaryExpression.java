package SQLExpression;

/**
 * This class is the second-level of the expression class hierarchy.
 * It handles the left and right child expressions.
 * It could be used for these operators: + - * /
 * @author messfish
 *
 */
public abstract class BinaryExpression extends Expression{

	private Expression leftchild;
	private Expression rightchild;
	
	/**
	 * Constructor: this constructor passes two child pointers
	 * and store them into two global variables.
	 * @param left the left child pointer.
	 * @param right the right child pointer.
	 */
	public BinaryExpression(Expression left, Expression right) {
		leftchild = left;
		rightchild = right;
	}
	
	/**
	 * this is the getter method for the left child.
	 * @return the left child for this expression.
	 */
	public Expression getLeftChild(){
		return leftchild;
	}
	
	/**
	 * this is the getter method for the right child.
	 * @return the right child for this expression.
	 */
	public Expression getRightChild(){
		return rightchild;
	}
	
	/**
	 * this method is the method for accepting visitor: just calls the 
	 * visitor in the CloneExpressionVisitor Interface. The logic of the tree
	 * traverse will be handled by the class who implement that interface.
	 * @param expression the visitor to be accepted.
	 */
	public void accept(AbstractVisitor visitor) {
		visitor.visit(this);
	}
	
	/**
	 * This method returns whether the node is a leaf node. At this time,
	 * we return false because all the expression class extends this class
	 * are not leaf nodes.
	 * @return a value shows whether this expression is a leaf node or not.
	 */
	@Override
	public boolean isLeaf() {
		return false;
	}
	
}
