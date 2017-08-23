package com.stackroute.datamunger.writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.stackroute.datamunger.query.DataSet;

public class JsonWriter {
	
	public boolean writeToJson(DataSet dataSet) {
		
		BufferedWriter writer=null;
		Gson gson=new GsonBuilder().setPrettyPrinting().create();
		String result=gson.toJson(dataSet);
		
		try {
			writer=new BufferedWriter(new FileWriter("resources/result.json"));
			writer.write(result);
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("File writing failed");
			
			return false;
		}
		finally {
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
		
	}

}
