import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BrandMap extends Mapper<LongWritable,Text,Text,NullWritable> {
	public void setup(Context context)
	{
		try {
			context.write(new Text("brand"), NullWritable.get());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void map(LongWritable key,Text value,Context context){
		if (Long.parseLong(key.toString()) == 0)
			return;
		else{
		String[] v = value.toString().split(",");
		String Brand = v[1];
        double Price = Double.parseDouble(v[3]);
		if (Price > 600){
			try {
				context.write(new Text(Brand), NullWritable.get());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	}
}
