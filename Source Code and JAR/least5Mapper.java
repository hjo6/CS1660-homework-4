import java.io.*; 
import java.util.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 

public class least5Mapper extends Mapper<Object, 
							Text, Text, LongWritable> { 

	private TreeMap<Long, String> tmap; 

	@Override
	public void setup(Context context) throws IOException, 
									InterruptedException 
	{ 
		tmap = new TreeMap<Long, String>(); 
	} 

	@Override
	public void map(Object key, Text value, 
	Context context) throws IOException, 
					InterruptedException 
	{ 

		String[] tokens = value.toString().split("\t"); 

		String movie_name = tokens[0]; 
		long no_of_views = Long.parseLong(tokens[1]); 

		// insert data into treeMap, 
		// we want least 5 viewed movies (or something) 
		// so we pass no_of_views (arbitrary name) as key 
		tmap.put(no_of_views, movie_name); 

		// we remove the last key-value 
		// if it's size increases 5 since
		// last key-value is the greatest
		// key 
		if (tmap.size() > 5) 
		{ 
			tmap.remove(tmap.lastKey()); 
		} 
	} 

	@Override
	public void cleanup(Context context) throws IOException, 
									InterruptedException 
	{ 
		for (Map.Entry<Long, String> entry : tmap.entrySet()) 
		{ 

			long count = entry.getKey(); 
			String name = entry.getValue(); 

			context.write(new Text(name), new LongWritable(count)); 
		} 
	} 
} 
