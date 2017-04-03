package SmallSQLServer;

import java.util.Scanner;

import SQLParser.PlainSelect;
import Support.Catalog;

/**
 * This is the top level of all the whole project.
 * That means, there will be a main function which runs the whole project.
 * Remember, the argument in the string is a file directory location. 
 * 
 * @author messfish
 *
 */
public class Main {

	private static String input_dir;
	// this string is the input directory of the file system.
	private static String output_dir;
	// this string is the output directory of the file system.
	private static String temp_dir;
	// this string is the temporary file directory of the file system.
	private static String test_dir;
	// this string is the testing file directory of the file system.
	private static int index = 1;
	// this index is used to tell the order of the query.
	
	/**
	 * this function is the setter method of the input directory string.
	 * @param s the input directory.
	 */
	public static void setInput(String s) {
		input_dir = s;
	}
	
	/**
	 * this function is the setter method of the temp directory string.
	 * @param s the temp directory.
	 */
	public static void setTemp(String s) {
		temp_dir = s;
	}
	
	/**
	 * this function is the getter method of the input directory string.
	 * @return the input directory.
	 */
	public static String getInput() {
		return input_dir;
	}
	
	/**
	 * this function is the getter method of the output directory string.
	 * @return the output directory.
	 */
	public static String getOutput() {
		return output_dir;
	}
	
	/**
	 * this function is the getter method of the temporary directory string.
	 * @return the temporary directory.
	 */
	public static String getTemp() {
		return temp_dir;
	}
	
	/**
	 * this function is the setter method of the test directory string.
	 * @param s the test directory.
	 */
	public static void setTest(String s) {
		test_dir = s;
	}
	
	/**
	 * this function is the getter method of the test directory string.
	 * @return the test directory.
	 */
	public static String getTest() {
		return test_dir;
	}
	
	/**
	 * This method is used to handle the string as the query. Generate
	 * the PlainSelect object and call the method from the QueryHandler
	 * to finish the rest.
	 * @param query the SQL query.
	 * @param index the order of the query.
	 * @param catalog the list of schemas available.
	 */
	public static void handleQuery(String query, int index, Catalog catalog) {
		long start = System.currentTimeMillis();
		PlainSelect plain = new PlainSelect(query);
		QueryHandler.handle(plain, index, catalog);
		long end = System.currentTimeMillis();
		long timeused = end - start;
		long numofseconds = timeused / 1000;
		long numofmilliseconds = timeused % 1000;
		System.out.println("Time usage on the query: " + numofseconds + 
				" s " + numofmilliseconds + " ms.");
	}
	
	/**
	 * this is the main method that runs the whole project.
	 * First, it scans the file presented as strings in the argument.
	 * Second, it uses a scanner to take the input of the user as query.
	 * Third, it let the SQLGrammer check whether this is a valid query.
	 * If it is valid, parse the SQL query into a parse tree.
	 * Last, it runs the query and prints the result if this is 
	 * a read query, connected with the time that spend on this query.
	 * @param args the argument stores the formated file directory.
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		input_dir = args[0] + "/input";
		output_dir = args[0] + "/output";
		temp_dir = args[0] + "/temp";
		test_dir = args[0] + "/test";
		while(true) {
			Catalog catalog = new Catalog();
			System.out.println("Please Enter a query: ");
			Scanner scan = new Scanner(System.in);
			String query = "", str = null;
			/* we will stop when we meet a line which ends at ";". */
			while((str=scan.nextLine())!=null) {
				if(str.length()>0&&str.charAt(str.length() - 1)==';') {
					query += str.substring(0, str.length() - 1);
					break;
				}
				query += str;
			}
			handleQuery(query, index, catalog);
			System.out.println("Do you want to continue? [y/n]");
			str = scan.nextLine();
			while(!str.equals("y")&&!str.equals("n")) {
				System.out.println("Invalid argument, please try again.");
				str = scan.nextLine();
			}
			if(str.equals("n")) {
				scan.close();
				System.out.println("Thank you for using the small SQL server.");
				break;
			}
			index++;
		}
	}
	
}
