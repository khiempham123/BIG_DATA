import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class SlidingWindowMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static final SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length < 3) return;

        String dateStr = parts[0].trim();
        String category = parts[1].trim();
        double amount = Double.parseDouble(parts[2].trim());
                context.write(new Text(dateStr + "\t" + category), new DoubleWritable(amount));

    }
}
