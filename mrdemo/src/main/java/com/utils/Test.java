package com.utils;

import java.util.ArrayList;

public class Test {
    public static void main(String[] args) {
        ArrayList list = new ArrayList();
        list.add(2);
        list.add(2);
        list.add(2);
        String[] split = list.toString().split(", ");
        for (int i = 0; i < split.length; i++) {
            System.out.println(split[i]);
        }
        System.out.println(list.toString());
        System.out.println(list);
    }
}
