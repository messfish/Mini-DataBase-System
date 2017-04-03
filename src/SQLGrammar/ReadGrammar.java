package SQLGrammar;

import Support.Catalog;

/**
 * In this class, it takes a string as SQL query language and check 
 * whether there is a syntax error in the query. It uses the automation
 * theory. Passing a set of states and evaluate the results.
 * @author messfish
 *
 */
public class ReadGrammar {

	private Catalog catalog = new Catalog();
	
	/**
	 * This method checks whether there is a grammar error in the query.
	 * It uses the automation theory to get the result.
	 * @return the boolean result shows whether the query is valid.
	 */
	/*
	public boolean checkTotal(String s) {
		int index = 0, state = 0;
		while(index < s.length()) {
			int parenthesis = 0; 
			switch(state){
			    // state 0 refers to the "SELECT" query.
			    // it must moves to the "FROM" query.
				case 0: int[] dummy = changeState(s,index,"FROM",1,parenthesis);
						if(dummy==null){
							System.out.println("Syntax error: could not find"
									+ "a FROM part, please check you set the"
									+ "right number of parenthesis or set the"
									+ "FROM part.");
							return false;
						}
						if(!checkSELECT(s.substring(index, dummy[0])))
							return false;
						state = dummy[1];
						break;
				// state 1 refers to the "FROM" query.
				// There are no specific query name it must follow.
				// However, it could end up in several parts: WHERE,
				// GROUP BY, ORDER BY. 
				case 1: String[] candidates = {"WHERE", "GROUP BY", "ORDER BY"};
						int[] array = null;
						for(int i=0;i<3;i++){
							array = changeState(s,index,candidates[i],2+i,parenthesis);
							if(array!=null) break;
				        }
						if(array==null){
							if(!checkFROM(s.substring(index))) 
								return false;
							else return true;
						}else{
							if(!checkFROM(s.substring(index, dummy[0])))
								return false;
						}
						state = dummy[1];
						break;
				// state 2 refer to the "WHERE" query.
				// There are no specific query name it must follow.
				// However, it could end up in several parts: GROUP BY, ORDER BY.
				case 2: String[] candidates1 = {"GROUP BY", "ORDER BY"};
						int[] array1 = null;
						for(int i=0;i<2;i++){
							array1 = changeState(s,index,candidates1[i],3+i,parenthesis);
							if(array1!=null) break;
						}
						if(array1==null){
							if(!checkWHERE(s.substring(index)))
								return false;
							else return true;
						}else{
							if(!checkFROM(s.substring(index, dummy[0])))
								return false;
						}
						state = dummy[1];
						break;
				// state 3 refer to the 
			}
		}
	}
	/*
	
	/**
	 * This function tries to get the target string. If it could,
	 * return an array as the result. If not, return null.
	 * @param s the query that will be evaluated.
	 * @param point the starting point of the string.
	 * @param target the target String we are looking for.
	 * @param nextState the next state will be returned if we find the target.
	 * @param parenthesis the number of parenthesis.
	 * @return an array with two elements, the first one is the ending point
	 * of the string part, the second one id the next state.
	 * Null means the method fails to get the desired target.
	 */
	private int[] changeState(String s, int point, String target,
							int nextState, int parenthesis) {
		while(point <= s.length() - target.length()) {
			String dummy = s.substring(point, point + target.length());
			String temp = dummy.toUpperCase();
			// only by the number of parenthesis is equal to 0 and the 
			// target string is find can we return the array.
			if(temp.equals(target)&&parenthesis==0)
				return new int[]{point, nextState};
			if(s.charAt(point)=='(') parenthesis++;
			if(s.charAt(point)==')') parenthesis--;
			point++;
		}
		return null;
	}
	
}
