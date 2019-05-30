package it.polito.bigdata.hadoop.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateIncome  implements org.apache.hadoop.io.Writable{

	private String date;
	private int income;
	
	
	//getter and setter
	public void setDate(String value) {
		date = value;
	}
	
	public void setIncome(int value) {
		income = value;
	}
	
	public String getDate() {
		return date;
	}
	
	public int getIncome() {
		return income;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		date = arg0.readUTF();
		income = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(date);
		arg0.writeInt(income);
	}

}
