package com.example.demo.service;

import java.util.Arrays;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;



import org.springframework.cloud.client.circuitbreaker.CircuitBreaker;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.example.demo.model.KafkaBankAccount;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class BankAccountService {

	
	@Autowired
	RestTemplate restTemplate	;
	
	@Autowired
    private CircuitBreakerFactory circuitBreakerFactory;
	
	@Value("${END_POINT_URL}")
    private String END_POINT_URL;
	
	@Value("${REACHABLE_END_POINT_URL}")
    private String REACHABLE_END_POINT_URL;
	
	@Value("${MONGO_END_POINT_URL}")
    private String MONGO_END_POINT_URL;
	
public void createAccount(KafkaBankAccount bankAccount) {
		
	System.out.println("Start createAccount.....  "  +bankAccount);
	
	
   try {
	HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
	restTemplate.postForObject(END_POINT_URL, request, KafkaBankAccount.class);
   }catch (Exception e) {
	   System.out.println("error calling the web service    "+e.getMessage());
   }
	
	
	
	
	}
	
	

	
@Retryable( value = Exception.class, 
		      maxAttempts = 3, backoff = @Backoff(delay = 1000))	
public boolean  createAccountRT(KafkaBankAccount bankAccount) throws Exception{
		
	System.out.println("Start createAccountRT.....  "  +bankAccount);
	HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
	restTemplate.postForObject(END_POINT_URL, request, KafkaBankAccount.class);
	return true;
  }

	
	@Recover
    boolean recover(Exception e, KafkaBankAccount bankAccount) {
		System.out.println("Start start Recover.....  "  +bankAccount);
		
		HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
		restTemplate.postForObject(MONGO_END_POINT_URL+"/accounts", request, KafkaBankAccount.class);
		return false;
	}
	
	
	public void  createAccountMDB(KafkaBankAccount bankAccount) throws Exception{
		
		System.out.println("Start createAccountMDB.....  "  +bankAccount);
		HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
		restTemplate.postForObject(MONGO_END_POINT_URL+"/accounts", request, KafkaBankAccount.class);
		
	  }

	
   public Object createAccountCB(KafkaBankAccount bankAccount) {
		
    	HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
    	CircuitBreaker circuitBreaker = circuitBreakerFactory.create("circuitbreaker");
         circuitBreaker.run(() -> restTemplate.postForObject(END_POINT_URL, request, KafkaBankAccount.class),throwable -> handleFailOver(bankAccount));
    	return null;
    }


   
   public Object handleFailOver(KafkaBankAccount bankAccount) {
	
	System.out.println("Start handleFailOver.....  "  +bankAccount);
	
	HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
	restTemplate.postForObject(MONGO_END_POINT_URL+"/accounts", request, KafkaBankAccount.class);
    return null;
	
	
	}


public boolean isEdgeServiceUp() {
	
    boolean result=false;	
    try {	
    	ResponseEntity<Boolean> response
  	  = restTemplate.getForEntity(REACHABLE_END_POINT_URL, Boolean.class);
    
    	result=true;	
    }catch(Exception e)
    {
    	System.out.println("Edge still down....");
    }
    	
    	
    	return result;
    	
    }


public void sendAllFromMongodb() {
	
	ResponseEntity<KafkaBankAccount[]> response
	  = restTemplate.getForEntity(MONGO_END_POINT_URL+"/accounts", KafkaBankAccount[].class);
	
	System.out.println("lenght for KafkaBankAccount[]="+response.getBody().length);
	
	
	Stream<KafkaBankAccount> accountStream = Arrays.stream(response.getBody());
	accountStream.forEach(a->{
		try {
			createAccountRT(a);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	});
	
	
	   restTemplate.getForEntity(MONGO_END_POINT_URL+"/accounts/deleteall", KafkaBankAccount[].class);
	   
	 
	
	
}



}
