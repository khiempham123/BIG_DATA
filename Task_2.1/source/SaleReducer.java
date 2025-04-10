import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SaleReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double total = 0.0;
        for (DoubleWritable val : values) {
            total += val.get();
        }

        total = Math.round(total * 100.0) / 100.0;
        context.write(key, new DoubleWritable(total));
    }
}


