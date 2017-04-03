package testcases;

import static org.junit.Assert.*;

import org.junit.Test;

import Support.TimeConversion;

/**
 * This class is used for testing whether the time conversion function
 * is correct or not. 
 * @author messfish
 *
 */
public class TimeConversionTest {

	private static final double DELTA = 1e-15;
	private TimeConversion convert = new TimeConversion();
	
	/**
	 * This method tests the conversion from time to number.
	 */
	@Test
	public void test1() {
		assertEquals(732.0, convert.fromTimeToNumber("00:12:12"), DELTA);
		assertEquals(47456.0, convert.fromTimeToNumber("13:10:56"), DELTA);
		assertEquals(32585.0, convert.fromTimeToNumber("09:03:05"), DELTA);
	}
	
	/**
	 * This method tests the conversion from number to time.
	 */
	@Test
	public void test2() {
		assertEquals("00:12:12", convert.fromNumberToTime(732.0));
		assertEquals("13:10:56", convert.fromNumberToTime(47456.0));
		assertEquals("09:03:05", convert.fromNumberToTime(32585.0));
	}

	/**
	 * this method tests the conversion from date to number.
	 */
	@Test
	public void test3() {
		assertEquals(730545.0, convert.fromDateToNumber("2000/03/01"), DELTA);
		assertEquals(767199.0, convert.fromDateToNumber("2100/07/09"), DELTA);
		assertEquals(670147.0, convert.fromDateToNumber("1834/10/20"), DELTA);
	}
	
	/**
	 * this method tests the conversion from number to date.
	 */
	@Test
	public void test4() {
		assertEquals("2000/03/01", convert.fromNumberToDate(730545.0));
		assertEquals("2100/07/09", convert.fromNumberToDate(767199.0));
		assertEquals("1834/10/20", convert.fromNumberToDate(670147.0));
	}
	
}
