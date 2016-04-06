package com.example.hive;

import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class CurrentTime extends UDF {
	
	private static Date dt = new Date();
	public Text evaluate(){
		
		return new Text(DateFormatUtils.format(dt, "HH:mm:ss"));
		
	}
}
