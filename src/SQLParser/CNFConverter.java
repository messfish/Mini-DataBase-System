package SQLParser;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

import SQLExpression.Expression;
import SQLExpression.MultiAndOperator;
import SQLExpression.MultiOrOperator;
import SQLExpression.NotOperator;
import SQLExpression.MultipleExpression;

/**
 * This class handles the conversion from a normal expression tree into
 * the CNF form. Basically it will follow these steps:
 * 
 * To help understanding, I will generate an example:
 * Here is the original tree:
 *                   OR
 *             /            \
 *            OR            NOT
 *         /     \           |
 *       NOT      H         AND
 *        |                /   \
 *       NOT              G    OR
 *        |                   /  \
 *        F                  H   NOT
 *                                |
 *                               OR
 *                             /    \
 *                            AND    L
 *                           /   \
 *                         ( )   ( )
 *                          |     |
 *                          J     K
 *                          
 * 1. rebuild the tree by replacing the "and" and "or" operators 
 * (which are binary) into their counterparts node that could hold 
 * multiple elements. Also, leave out the parenthesis node to
 * make the tree uniform.
 * 
 * After the transform, the result should be like this:
 *                   OR(M)
 *             /            \
 *            OR(M)         NOT
 *         /     \           |
 *       NOT      H         AND(M)
 *        |                /   \
 *       NOT              G    OR(M)
 *        |                   /  \
 *        F                  H   NOT
 *                                |
 *                               OR(M)
 *                             /    \
 *                            AND(M)  L
 *                           /   \
 *                          J     K
 * 
 * 2. push the not operators into the bottom of the expression. That
 * means the not operator will be the root of the expression tree
 * where no "and" or "or" exists. Be sure use the De Morgan's law
 * and double not law.
 * 
 * How to use De Morgan law:
 * For example, here is the original expression tree:
 *                NOT
 *                 |
 *                AND(M)
 *              /     \
 *             G       H
 *             
 * After we use the De Morgan law, the result should be like this:
 *                OR(M)
 *              /     \
 *            NOT     NOT
 *             |       |
 *             G       H
 * 
 * After the transform, the result should be like this:
 *                   OR(M)
 *              /             \
 *            OR(M)           OR(M)    
 *          /    \          /       \
 *         F      H       NOT       AND(M)
 *                         |       /   \
 *                         G      NOT  OR(M)
 *                                 |  /     \
 *                                 H AND(M)  L
 *                                   /  \
 *                                  J    K
 *  
 * 3. gather all the adjacent "and" or "or" operator together.
 * After doing that, the expression tree will be presented as:
 * all the and expression will be in either odd or even levels,
 * this will be the same for the or operator.
 * 
 * After the transform, the expression tree should be like this:
 *                         OR(M)
 *               /     /         \     \
 *              F     H          NOT   AND(M)
 *                                |   /    \
 *                                G  NOT   OR(M)
 *                                    |   /   \ 
 *                                    H  AND(M) L
 *                                      /     \
 *                                     J       K
 *  
 * 4. push the and operator upwards until the root is an and 
 * operator and all the children are or operators with multiple
 * components. At this time we get the result: an expression in CNF form.
 * How do we push and up? Use distribution law!
 *  
 * For example, here is the way to push the and up and merge them.
 *                       OR
 *                    /      \
 *                  AND       L
 *                /     \
 *               J       K
 *               
 * In the normal form, it could be: (J AND K) OR L.
 * If we apply the distribution law, we will get the result like this:
 * (J OR L) AND (K OR L), the tree form of this should be like:
 *                     AND
 *                   /     \
 *                  OR     OR
 *                /   \   /   \
 *               J     L K     L
 *               
 * So after we push the AND at the deepest level up and merge it with the 
 * existing add, we get this result.
 *                     OR(M)
 *           /     /         \              \
 *          F     H          NOT            AND(M)
 *                            |      /       |       \
 *                            G    NOT      OR(M)    OR(M)
 *                                  |      /    \   /    \
 *                                  H     J     L  K      L 
 * 
 * Now let us push the and up and we will get the result like this:
 *                             AND(M)
 *             /                |                 \
 *            OR(M)             OR(M)             OR(M)
 *     /    /   \   \    /   /  |   \   \    /  /  |   \  \
 *    F    H    NOT NOT  F  H  NOT  J   L   F  H  NOT  K   L
 *               |   |          |                  |
 *               G   H          G                  G
 * 
 * @author messfish
 *
 */
public class CNFConverter {

	private Expression root; 
	// the variable that stores the newly generated root.
	private Expression dummy;
	// this variable mainly serves as the dummy root of the true root.
	// generally it will be a multi and operator with root as the child.
	private Expression temp1, temp2, child;
	// these two variable mainly serves as nodes that traverse through
	// the expression tree to change the structure of expression tree.
	// notice temp1 will be settled as the root and temp2 will be 
	// settled as the dummy root.
	private CloneExpression clone = new CloneExpression();
	// this will be used for cloning the tree.
	
	/**
	 * this class is mainly used for gather the parent expression,
	 * children expression and the level of the children expression
	 * in the expression tree together.
	 * @author messfish
	 *
	 */
	private class Mule {
		private Expression parent;
		private Expression child;
		private int level;
		private Mule(Expression parent, Expression child, int level) {
			this.parent = parent;
			this.child = child;
			this.level = level;
		}
	}
	
	/**
	 * this method takes an expression tree and converts that into
	 * a CNF form. Notice the 4 steps shown above will turn into
	 * 4 different methods. For the sake of testing, I set them public.
	 * return the converted expression.
	 * @param express the original expression tree.
	 */
	public void convert(Expression express) {
		reorder(express);
		pushNotDown();
		/* notice for the gather() function, we do not change the variable
		 * that points to the root by pointing to others. Also, we do not 
		 * change those temp variables. So there is no need to set those
		 * variables back to their modified state. */
		gather();
		pushAndUp();
	}
	
	/**
	 * this is the getter method of the root expression.
	 * @return the root expression.
	 */
	public Expression getRoot() {
		return root;
	}
	
	/**
	 * this is the first step that rebuild the expression tree.
	 * Use the standard specified in the above class. Traverse the 
	 * original tree recursively and rebuild the tree from that.
	 * I will use a single class that implement the clone expression visitor
	 * to handle this problem. 
	 * @param express the original expression tree.
	 */
	public void reorder(Expression express) {
		root = clone.clone(express);
		List<Expression> list = new ArrayList<>();
		list.add(root);
		dummy = new MultiAndOperator(list);
	}
	
	/**
	 * This method is used to deal with pushing not operators down.
	 * Since it needs an extra parameter, I will create a new 
	 * method to handle this.
	 */
	public void pushNotDown() {
		/* set the two temp parameters to their staring point. */
		temp1 = root;
		temp2 = dummy;
		/* I set it to zero since if the modification happens at the root,
		 * the parent will have the correct pointer to the children. */
		pushNot(0);
		/* do not forget to set the operators back! */
		root = ((MultiAndOperator)dummy).getChild(0);
		temp1 = root;
		temp2 = dummy;
	}
	
	/**
	 * This method is the helper function to push not operators down.
	 * traverse the tree thoroughly, when we meet the not operator.
	 * We only need to consider these three operators: MultiAndOperator,
	 * MultiOrOperator, NotOperator. Handle them in a seperate way.
	 * when we finish the traverse, the expression tree will have 
	 * all the not operators pushed as downwards as they could.
	 * In the method, I use two global variables: temp1 and temp2 
	 * to traverse the expression tree. Notice that temp2 will always
	 * be the parent of temp1.
	 * @param index the index of the children appeared in parents array.
	 */
	private void pushNot(int index) {
		/* what really matters is the three logical operators:
		 * and, or, not. so we only deal with these three operators. */
		if(temp1 instanceof MultiAndOperator) {
			MultiAndOperator and = (MultiAndOperator)temp1;
			for(int i=0; i< and.size(); i++) {
				temp2 = and;
				temp1 = and.getChild(i);
				pushNot(i);
			}
		}else if(temp1 instanceof MultiOrOperator) {
			MultiOrOperator or = (MultiOrOperator)temp1;
			for(int i=0; i< or.size(); i++) {
				temp2 = or;
				temp1 = or.getChild(i);
				pushNot(i);
			}
		}
		else if(temp1 instanceof NotOperator) {
			handleNot(index);
		}
	}
	
	/**
	 * This function mainly deals with pushing not operators down. 
	 * check the child. If it is not a logic operator(and or or).
	 * stop at that point. Else use De Morgan law to push not downwards.
	 * @param index the index of the children appeared in parents array.
	 */
	private void handleNot(int index) {
		child = ((NotOperator)temp1).getChild();
		int nums = 1; // takes down the number of not operators.
		while(child instanceof NotOperator){
			child = ((NotOperator)child).getChild();
			nums++;
		}
		/* if the number of not operators are even. we could get
		 * rid of all the not operators. set the child to the parent. */
		if(nums%2==0) {
			((MultipleExpression)temp2).setChild(index, child);
			temp1 = child;
			pushNot(-1);
		}
		/* otherwise there will be one not left to push. */
		else{
			/* if the child is not these two types of operators.
			 * that means we reach the leaves of the logical part.
			 * set a new not operator whose child is the current one
			 * and connect that operator with the parent and return. */
			if(!(child instanceof MultiAndOperator) &&
					!(child instanceof MultiOrOperator)){
				child = new NotOperator(child);
				((MultipleExpression)temp2).setChild(index, child);
				return;
			}else if(child instanceof MultiAndOperator) {
				MultiAndOperator and = (MultiAndOperator)child;
				List<Expression> list = new ArrayList<>();
				for(int i=0;i<and.size();i++) {
					/* push not to every element in the operator. */
					NotOperator not = new NotOperator(and.getChild(i));
					list.add(not);
				}
				/* the De Morgan law shows we need to change and to or. */
				temp1 = new MultiOrOperator(list);
				((MultipleExpression)temp2).setChild(index, temp1);
				pushNot(-1);
			}else if(child instanceof MultiOrOperator) {
				MultiOrOperator or = (MultiOrOperator)child;
				List<Expression> list = new ArrayList<>();
				for(int i=0;i<or.size();i++) {
					/* push not to every element in the operator. */
					NotOperator not = new NotOperator(or.getChild(i));
					list.add(not);
				}
				/* the De Morgan law shows we need to change or to and. */
				temp1 = new MultiAndOperator(list);
				((MultipleExpression)temp2).setChild(index, temp1);
				pushNot(-1);
			}
		}
	}
	
	/**
	 * This method serves as dealing with the third step. It is used
	 * to put all the adjacent same multi operators together. BFS the 
	 * tree and do it node by node. In the end we will get the tree
	 * where all the same multi operators store in the same odd level
	 * of the tree or in the same even level of the tree.
	 */
	public void gather() {
		Queue<Expression> queue = new LinkedList<>();
		queue.offer(temp1);
		while(!queue.isEmpty()) {
			Expression express = queue.poll();
			/* at this level, we only deal with "multi and" and "multi or"
			 * operators, so we only consider these two operators. 
			 * that means we do nothing if the operator is not those two. */
			if(express instanceof MultiAndOperator) {
				MultiAndOperator and = (MultiAndOperator)express;
				while(true) {
					int index = 0;
					Expression get = null;
					for(;index<and.size();index++) {
						get = and.getChild(index);
						if(get instanceof MultiAndOperator)
							break;
					}
					/* if the index is the size of the multi operator,
					 * that means this is already valid. jump out of the loop. */
					if(index==and.size()) break;
					/* if not, remove the child out and push the child of that child
					 * in the operator, starting from the index where the child 
					 * is removed. */
					else{
						and.removeChild(index);
						MultipleExpression order = (MultipleExpression)get;
						for(int i=0;i<order.size();i++){
							and.addChild(index, order.getChild(i));
							index++;
						}
					}
				}
				/* Do the standard BFS now since all children are not and operators. */
				for(int i=0;i<and.size();i++)
					queue.offer(and.getChild(i));
			}
			/* for the multi or operator, the logic is the similar. */
			else if(express instanceof MultiOrOperator) {
				MultiOrOperator or = (MultiOrOperator)express;
				while(true) {
					int index = 0;
					Expression get = null;
					for(;index<or.size();index++) {
						get = or.getChild(index);
						if(get instanceof MultiOrOperator)
							break;
					}
					/* if the index is the size of the multi operator,
					 * that means this is already valid. jump out of the loop. */
					if(index==or.size()) break;
					/* if not, remove the child out and push the child of that child
					 * in the operator, starting from the index where the child 
					 * is removed. */
					else{
						or.removeChild(index);
						MultipleExpression order = (MultipleExpression)get;
						for(int i=0;i<order.size();i++){
							or.addChild(index, order.getChild(i));
							index++;
						}
					}
				}
				/* Do the standard BFS now since all children are not or operators. */
				for(int i=0;i<or.size();i++)
					queue.offer(or.getChild(i));
			}
		}
	}
	
	/**
	 * Generally speaking this could be the last step in the conversion to CNF.
	 * First, BFS the tree and gather all the or operators and their parents
	 * into a stack. Next, pop them out and push the and operators under the 
	 * or operators upwards(if there are). Do this level by level, which means
	 * during each level we will call the gather() method to make the tree uniform.
	 * When we move out of the stack. The expression tree shall be in CNF form.
	 */
	public void pushAndUp() {
		Queue<Mule> queue = new LinkedList<>();
		Stack<Mule> stack = new Stack<>();
		Mule root = new Mule(temp2, temp1, 0);
		queue.offer(root);
		int level = 1;
		/* do the BFS and store valid mule into the stack. Notice the 
		 * first parameter is parent and the second parameter is children. */
		while(!queue.isEmpty()) {
			int size = queue.size();
			for(int i=0; i<size; i++) {
				Mule mule = queue.poll();
				Expression parent = mule.parent, child = mule.child;
				if(parent instanceof MultiAndOperator &&
					child instanceof MultiOrOperator)
					stack.push(mule);
				/* Note the child may not be an instance of multiple expression!. */
				if(child instanceof MultipleExpression) {
					MultipleExpression multi = (MultipleExpression)child;
					for(int j=0;j<multi.size();j++) {
						Expression get = multi.getChild(j);
						if(get instanceof MultipleExpression) {
							Mule added = new Mule(child, get, level);
							queue.offer(added);
						}
					}
				}
			}
			level++;
		}
		/* use another function to handle pushing and up. */
		pushAnd(stack);
		/* do not forget to set the operators back! */
		this.root = ((MultiAndOperator)dummy).getChild(0);
		temp1 = this.root;
		temp2 = dummy;
		/* at last, remember to gather again since there are no gather()
		 * method called if there are some movements on the root. */
		gather();
	}
	
	/**
	 * This helper function is used to deal with pushing and up:
	 * generally, pop the top element out of the stack,
	 * use BFS to traverse the tree and push and up.
	 * It will case the expression tree to have the and as the new 
	 * root and multiple or as the children. Push them on the queue
	 * and repeat the same process until the newly generated or 
	 * operator does not have any and operators in it(which means no
	 * elements will be added into the queue). when one level is 
	 * finished, regroup the tree. Do this until the stack is empty,
	 * the result will be the expression in CNF form. 
	 * @param stack the stack stores a list of combined data.
	 */
	private void pushAnd(Stack<Mule> stack) {
		int level = 0;
		if(!stack.isEmpty()) level = stack.peek().level;
		while(!stack.isEmpty()) {
			Mule mule = stack.pop();
			/* we finish a level, uniform the tree by calling gather. */
			if(level!= mule.level) {
				gather();
				level = mule.level;
			}
			Queue<Mule> queue = new LinkedList<>();
			/* this time we do not need to take down the level of the
			 * tree, so simply set a 0 to the last parameter. */
			Mule combined = new Mule(mule.parent, mule.child, 0);
			queue.offer(combined);
			while(!queue.isEmpty()) {
				Mule get = queue.poll();
				Expression parent = get.parent, child = get.child;
				/* based on the code above, the stack only have the expression
				 * which they are multi operators. so safely convert them. */
				MultipleExpression children = (MultipleExpression)child;
				int index = 0; 
				MultiAndOperator and = null;
				/* find the children that the child is an multi and operator. */
				for(;index<children.size();index++) {
					if(children.getChild(index) instanceof MultiAndOperator) {
						and = ((MultiAndOperator)children.getChild(index));
						break;
					}
				}
				if(index==children.size()) continue;
				children.removeChild(index);
				MultipleExpression parents = (MultipleExpression)parent;
				List<Expression> list = new ArrayList<>();
				MultiAndOperator newand = new MultiAndOperator(list);
				parents.setChild(parents.getIndex(children), newand);
				for(int i=0;i<and.size();i++) {
					Expression temp = clone.clone(children);
					MultipleExpression mtemp = (MultipleExpression)temp;
					mtemp.addChild(mtemp.size(), and.getChild(i));
					newand.addChild(i, mtemp);
					queue.offer(new Mule(newand, mtemp, 0));
				}
			}
		}
	}
	
}
