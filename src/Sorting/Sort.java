package Sorting;

import java.io.File;
import java.util.Map;

import Support.Mule;

/**
 * This is the abstract class that mainly handles sort. Normally
 * there are two types of sort: In memory sort and the external sort.
 * No matter what types of sorting you use. Be sure to put the 
 * results into a TempOperator and fetch the schema.
 * @author messfish
 *
 */
public abstract class Sort {

	/**
	 * This method is used to get the result file out.
	 * @return the file contains the sorted data.
	 */
	public abstract File getResult();
	
	/**
	 * This is the getter method of the schema of the operator.
	 * @return the schema of the file.
	 */
	public abstract Map<String, Mule> getSchema();
	
}
