package com.example.hive;

import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class CurrentDate extends UDF {
	private static Date dt = new Date();

	public Text evaluate(){
		//hive date format
		return new Text(DateFormatUtils.format(dt, "yyyy-MM-dd"));
		
	}
}
