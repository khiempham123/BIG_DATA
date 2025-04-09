import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SlidingWindowReducer extends Reducer<Text, DoubleWritable, NullWritable,Text> {

    private TreeMap<String, Map<String, Double>> data = new TreeMap<>();
    private SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");
    private boolean headerWritten = false;
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException {
        String[] parts = key.toString().split("\t");
        if (parts.length < 2) return;

        String date = parts[0];
        String category = parts[1];

        double total = 0.0;
        for (DoubleWritable val : values) {
            total += val.get();
        }

        data.putIfAbsent(date, new HashMap<>());
        data.get(date).put(category, total);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<String> allCategories = new HashSet<>();
        for (Map<String, Double> map : data.values()) {
            allCategories.addAll(map.keySet());
        }

	if (!headerWritten) {
    		context.write(NullWritable.get(), new Text("report_date,category,revenue"));
    		headerWritten = true;
	}
        // Lấy ngày lớn nhất (cuối cùng) trong dữ liệu đầu vào
        String lastDate = data.lastKey();

        // Tạo danh sách tất cả ngày từ nhỏ nhất -> ngày cuối cùng + 2 ngày
        List<String> allDates = new ArrayList<>();
        String current = data.firstKey();

        while (compareDates(current, addDays(lastDate, 2)) <= 0) {
            allDates.add(current);
            current = addDays(current, 1);
        }

        for (String date : allDates) {
            for (String category : allCategories) {
                double revenue = 0.0;
                for (int i = 0; i < 3; i++) {
                    String prevDate = addDays(date, -i); // D, D−1, D−2
                    Map<String, Double> catMap = data.getOrDefault(prevDate, new HashMap<>());
                    revenue += catMap.getOrDefault(category, 0.0);
                }

                revenue = Math.round(revenue * 100.0) / 100.0;
            	String output = String.format("%s,%s,%.2f", date, category, revenue);
            	context.write(NullWritable.get(), new Text(output));
            }
        }
    }

    private String addDays(String dateStr, int days) {
        try {
            Date date = format.parse(dateStr);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.DATE, days);
            return format.format(cal.getTime());
        } catch (Exception e) {
            return dateStr;
        }
    }

    private int compareDates(String d1, String d2) {
        try {
            Date date1 = format.parse(d1);
            Date date2 = format.parse(d2);
            return date1.compareTo(date2);
        } catch (Exception e) {
            return 0;
        }
    }
}
