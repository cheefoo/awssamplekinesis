package com.tayo.awssamplekinesis;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by temitayo on 8/30/17.
 */
public class DataUtils
{
   public static List<String> retrieveDataLines(String fileName) throws Exception
   {
       List<String> dataList = new ArrayList<String>();
       FileInputStream fis = new FileInputStream(new File(fileName));
       BufferedReader br = new BufferedReader(new InputStreamReader(fis));
       String line = null;
       while((line = br.readLine()) != null)
       {
           dataList.add(line);
       }

       return dataList;
   }
}
