package com.nfsoftTest.testCleanFromKafka;

import org.apache.flink.runtime.operators.resettable.SpillingResettableIterator;

public class testRep {
    public static String repStr(String orginStr){
        String ret = orginStr.replaceAll("\\\\", "");
        return ret;
    }

    public static void main(String[] args) {
        String test = "str = {\\\"name\\\":\\\"spy\\\",\\\"id\\\":\\\"123456\\\"}";
        System.out.println(test);
        String retString = repStr(test);
        System.out.println(retString);
    }
}
