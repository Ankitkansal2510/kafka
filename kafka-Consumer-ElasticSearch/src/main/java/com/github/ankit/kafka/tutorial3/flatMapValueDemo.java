package com.github.ankit.kafka.tutorial3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class flatMapValueDemo {
    public static void main(String[] args) {
        List<String> list= Arrays.asList("A,b,c");
        List<String> list1= Arrays.asList("Aa,bb,cc");
        List<String> list2= Arrays.asList("Aaa,bbb,ccc");
        List<String> list3= Arrays.asList("Aaaa,bbbb,cccc");

        List<List<String>> secondList=new ArrayList<>();
        secondList.add(list);
        secondList.add(list1);
        secondList.add(list2);
        secondList.add(list3);

        List<String> first=new ArrayList<>();
        for(List<String> combinedList:secondList)
        {
            for(String hname:combinedList){
            first.add(hname);
        }}

        System.out.println(first);

        List<String> flatMapList=secondList.stream().flatMap(hlist->hlist.stream()).collect(Collectors.toList());


        System.out.println("\nPrinting through flapmap");
        System.out.println(flatMapList);


        List<Integer> arr=Arrays.asList(1,2,3,4,5,6,7,8,9);
       List<Integer> arr1=arr.stream().filter(value->value>5).collect(Collectors.toList());
       List<Integer> arr2=arr.stream().map(value->value*5).collect(Collectors.toList());
        System.out.println("Printing filtered value " + arr1);
        System.out.println("\nPrinting new map value " + arr2);


    }
}
