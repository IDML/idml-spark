package org.idml.spark;

import org.apache.spark.api.java.function.Function;

import com.datasift.ptolemy.Ptolemy;
import com.datasift.ptolemy.PtolemyJson;
import com.datasift.ptolemy.PtolemyMapping;
import com.datasift.ptolemy.PtolemyValue;

public final class IDMLMap implements Function<String,String> {

String _idmlPath;
	
	public IDMLMap(String idmlPath)
	{
		_idmlPath = idmlPath;
	}

	public String call(String value) throws Exception {
		
		try
    	{
			//TODO: How can I persist Ptolemy / Ptolemy Mapping so we don't have to keep recreating these?
        	Ptolemy ptolemy = new Ptolemy();
			PtolemyMapping mapper;
			mapper = ptolemy.fromFile(_idmlPath);
			
    		PtolemyValue mapped = mapper.run(PtolemyJson.parse(value));
            return PtolemyJson.compact(mapped);
    	}
    	catch(Exception ex)
    	{
    		//TODO: Where to log exceptions?
    		ex.printStackTrace(System.err);
    		return null;
    	}
		
	}
	
}
