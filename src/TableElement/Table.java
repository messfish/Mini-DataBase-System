package TableElement;

import SQLParser.PlainSelect;

/**
 * This is the class that handles the table. The table
 * could either be atomic (a specific table) or a nested query.
 * (That means there will be a plain select in there).
 * @author messfish
 *
 */
public class Table {
	
	private PlainSelect select;
	// this variable stores the table which is a select query.
	private String atom;
	// this variable stores the table which is a table name.
	private String name;
	// this variable stores the name of the table.
	
	/**
	 * Constructor: this constructor handles the table which
	 * is a select query. 
	 * @param name the name of the select query.
	 * @param select a table as a select query
	 */
	public Table(String name, PlainSelect select) {
		this.select = select;
		this.name = name;
	}
	
	/**
	 * Constructor: This constructor handles the table which is
	 * a table name.
	 * @param atom a table name
	 */
	public Table(String atom) {
		this.atom = atom;
		name = atom;
	}
	
	/**
	 * this is the getter method of the name of the table.
	 * @return the name of the table.
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * This method is used to check whether two tables are equal or
	 * not. we need to handle different types separately since there
	 * are two different types of the table elements.
	 * @param that the table that used to be checked.
	 * @return the boolean value shows the two tables are equal or not.
	 */
	public boolean equals(Table that) {
		if(that.atom!=null) {
			if(atom==null) return false;
			else return atom.equals(that.atom);
		}
		if(that.select!=null) {
			if(select==null) return false;
			else return select.equals(that.select) && name.equals(that.name);
		}
		return false;
	}

}
