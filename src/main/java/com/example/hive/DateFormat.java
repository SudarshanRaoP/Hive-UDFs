package com.example.hive;

import java.text.ParseException;
import java.util.Date;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.DateFormatUtils;


public class DateFormat extends UDF {

	@Description(name= "_FUNC_",
				value="_FUNC_(string) - Coverts date string into standard mm/dd/yy format",
				extended="SELECT _FUNC_(col) FROM table")
	
	public Text evaluate(Text s) {
		try {
		//Parse Input date string
		Date _date = DateUtils.parseDate( s.toString(), "MMM d, yyyy", "MMM d, yy", "MM/d/yyyy", "MM/d/yy", "M/d/yyyy", "M/d/yyyy", "M/d/yy");
		//Format Date
		String _formatted = DateFormatUtils.format(_date, "MM/dd/yy");

		return new Text(_formatted);
		}
		catch (ParseException pe) {
			return s;
		}

	}
}