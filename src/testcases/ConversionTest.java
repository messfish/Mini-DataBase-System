package testcases;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import SmallSQLServer.Main;
import Support.BinaryToHuman;
import Support.HumanToBinary;
import Support.RandomTable;
import Support.checkFile;

/**
 * Like the name suggests, this class is mainly used for
 * checking whether the conversion between the binary and
 * human readable is correct or not.
 * @author messfish
 *
 */
public class ConversionTest {

	@Test
	public void test() {
		Main.setTest("/Users/messfish/Desktop/SQLdatabase/test/");
		Main.setInput("/Users/messfish/Desktop/SQLdatabase/input/");
		RandomTable random = new RandomTable(1000000);
		/* check it multiple times to guarantee our code works. */
		for(int i = 0; i < 100 ; i++) {
			File file = random.generate(1);
			HumanToBinary human = new HumanToBinary();
			File temp = human.convert(file, "Test");
			BinaryToHuman binary = new BinaryToHuman();
			File result = binary.convert(temp, "Test"); 
			checkFile check = new checkFile();
			assertTrue(check.checkHumanEqual(file, result));
		}
	}

}
