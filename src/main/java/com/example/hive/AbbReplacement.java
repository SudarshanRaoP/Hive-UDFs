package com.example.hive;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class AbbReplacement extends UDF {
	
	private Map<String, String> abbrmap;
	private Set<String> abbrkeys;
	
	public AbbReplacement(){
		this.abbrmap = new HashMap<String, String>();
		abbrmap.put("Ft", "Fort");
		abbrmap.put("Pk", "Park");
		abbrmap.put("Mtn", "Mountain");
		
		this.abbrkeys = abbrmap.keySet();
	}
	
	public Text evaluate(Text s){
		
		Text out = new Text();
		
		String in = s.toString();
		
		for(String abb: abbrkeys){
			Pattern p = Pattern.compile(abb);
			Matcher m = p.matcher(in);
			if (m.find()){
				String str_out = in.replaceAll(abb, abbrmap.get(abb));
				out.set(str_out);
				return out;
			}
			
	}
		return s;
	}	
}

