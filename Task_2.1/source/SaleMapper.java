import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Calendar;
public class SaleMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static final SimpleDateFormat inputFormat = new SimpleDateFormat("MM-dd-yy");
    private static final SimpleDateFormat outputFormat = new SimpleDateFormat("dd/MM/'20'yy");
    @Override
    protected void setup(Context context) {
        // Đảm bảo năm 2 chữ số bắt đầu từ 2000 trở đi
        inputFormat.set2DigitYearStart(new GregorianCalendar(2000, Calendar.JANUARY, 1).getTime());
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("index") || line.trim().isEmpty()) return;

        String[] fields = line.split(",", -1); // allow empty fields

        if (fields.length < 16) return;

        String rawDate = fields[2].trim();
        String status = fields[3].trim().toLowerCase();
        String category = fields[9].trim();
        String amountStr = fields[15].trim();

        if (!status.equals("shipped")) return;
	System.err.println("DEBUG: rawDate=" + rawDate + ", status=" + status + ", category=" + category + ", amountStr=" + amountStr);

        try {
            	Date baseDate = inputFormat.parse(rawDate);
            	String formattedDate = outputFormat.format(baseDate);
            	double amount = Double.parseDouble(amountStr);
		Text outputKey = new Text(formattedDate + "\t" + category);
                context.write(new Text(outputKey), new DoubleWritable(amount));
    	}
        catch (Exception e) {
                System.err.println("ERROR parsing line: " + line);
    		e.printStackTrace();
        }
    }
}

