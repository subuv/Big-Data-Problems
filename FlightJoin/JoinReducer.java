import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException , InterruptedException {
		ArrayList<String> ListA = new ArrayList<String>();
		ArrayList<String> ListB = new ArrayList<String>();
		String keys = key.toString();
		
		if(key.equals("A") {
			ListA.add(keys);
		} else if(key.equals("B") {
			ListB.add(keys);
		}
		
		for(String elementA : ListA) {
			for(String elementB : ListB) {
				if(elementA.equals(elementB) {
					String[] elemA = elementA.split(",");
					String[] elemB = elementB.split(",");
					int hourA = elemA[3];
					int hourB = elemB[3];

					if(((hourB - hourA) >= 1) && ((hourB - hourA) <= 5)) {
						String val = elemA[9] + "\t" + elemB[9] + "\t" + elemA[16] + "\t" + elemA[17] + "\t" + elemB[16] + "\t" 									+ elemB[17] + "\t" + elemA[7] + "\t" + elemB[5];
						context.write(val, null);
					}
				}			
			}
		}
	}
}