package com.fanli.bigdata;

import java.sql.Types;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by laichao.wang on 2018/12/29.
 */
public class TestT {
    String autoIncKeyEnd="5";

    public TestT(String autoIncKeyEnd) {
        this.autoIncKeyEnd = autoIncKeyEnd;
    }

    /**
     * 重新获取最大值
     * @param min
     * @param max
     * @param dtype
     * @return
     */
    private long resetMax(long min, long max, int dtype) {
        try {
            if( dtype == Types.TIMESTAMP
                    ||
                    (String.valueOf(max).length() == String.valueOf(min).length() && (String.valueOf(min).length() == 13))
                    ){
                return getEndMax(1000,max);
            }else if(String.valueOf(max).length() == String.valueOf(min).length() && (String.valueOf(min).length() == 10)){
                return getEndMax(1,max);
            }else{

            }
        }catch (Exception e){
        }
        return max;
    }

    /**
     * @param i 扩大倍数
     * @return
     */
    private long getEndMax(int i,long max) throws Exception{
        if(autoIncKeyEnd.equals("now")){
            return ( System.currentTimeMillis())/1000*i;
        }
        if(!autoIncKeyEnd.startsWith("now") ){
            if(autoIncKeyEnd.startsWith("|")){
                autoIncKeyEnd="-"+autoIncKeyEnd;
            }else{
                autoIncKeyEnd="-|"+autoIncKeyEnd;
            }
        }
        String[] split = autoIncKeyEnd.split("\\|");
        Calendar instance = Calendar.getInstance();
        if("now".equals(split[0])){
            instance.setTime(new Date());
        }else{
            instance.setTime(new Date(max/i*1000));
        }
        instance.add(Calendar.HOUR_OF_DAY,Integer.parseInt(split[1]));
        return instance.getTime().getTime()/1000*i;
    }

    public static void main(String[] args) {
        TestT testT = new TestT("now|1");
        long max = testT.resetMax(1545977527L, 1546063927L, 3);
        System.out.println(max);
    }

}
