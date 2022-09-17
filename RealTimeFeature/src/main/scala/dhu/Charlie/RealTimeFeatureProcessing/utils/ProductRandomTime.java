package dhu.Charlie.RealTimeFeatureProcessing.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class ProductRandomTime {

    public static long getRandomTimeStamp(String beginTime, String endTime){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        long longTime = 0;
        try {
            Date d1 = sdf.parse(beginTime);
            long before = d1.getTime();
            //获取当前时间
            Date d2 = sdf.parse(endTime);
            long after = d2.getTime();
            longTime = randomLong(before, after);
        }
        catch (ParseException e) {
            e.printStackTrace();
        }
        return longTime;
    }

    public static String getFormatRandomTime(String beginTime, String endTime){
        long t = getRandomTimeStamp(beginTime, endTime);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(t);
    }

    private static long randomLong(long start, long end){
        Random r = new Random();
        return (long) (start + (r.nextFloat() * (end - start + 1)));
    }

}
