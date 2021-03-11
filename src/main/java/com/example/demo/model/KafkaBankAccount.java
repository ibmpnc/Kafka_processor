package com.example.demo.model;

import java.io.Serializable;
import java.math.BigDecimal;

import javax.xml.bind.annotation.XmlRootElement;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;



@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaBankAccount implements Serializable{

	
	


	private  String balance;
	private  String name;
	private  String address;
	private  String accountType;
	
	

	@Override
	    public String toString() {
	        return "bank_account{" +
	                "name='" + name + '\'' +
	                ", address='" + address + '\'' +
	                "balancd='" + balance+ '\'' +
	                "account_type='" + accountType + '\'' +
	                '}';
	    }
}
