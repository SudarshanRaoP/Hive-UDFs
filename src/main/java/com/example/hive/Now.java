package com.example.hive;

import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Now extends UDF {

	private static Date dt = new Date();
	public Text evaluate(){
		//Format have to be standard hive timestamp format otherwise hive datetime function will not work.
		return new Text(DateFormatUtils.format(dt, "yyyy-MM-dd HH:mm:ss"));
		
	}
}

