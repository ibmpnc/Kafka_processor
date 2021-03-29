package com.example.demo.service;

import java.time.Duration;
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
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;

import com.example.demo.model.KafkaBankAccount;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

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
	
	
	@Value("${EDGE_END_POINT_URL}")
    private String EDGE_END_POINT_URL;
	
	
	
public void createAccount(KafkaBankAccount bankAccount) {
		
	log.info("Start createAccount.....  "  +bankAccount);
	

	
	
   try {
	HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
	restTemplate.postForObject(END_POINT_URL, request, KafkaBankAccount.class);
   }catch (Exception e) {
	   log.error("error calling the web service    "+e.getMessage());
   }
	
	
	
	
	}
	
	

	
@Retryable( value = Exception.class, 
		      maxAttempts = 3, backoff = @Backoff(delay = 1000))	
public boolean  createAccountRT(KafkaBankAccount bankAccount) throws Exception{
		
	log.info("Start createAccountRT.....  "  +bankAccount);
	HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
	KafkaBankAccount account =restTemplate.postForObject(END_POINT_URL, request, KafkaBankAccount.class);
	
	log.info("Returned from edge :: "+account.toString());
	return true;
  }




boolean reslut=true;
public boolean  createAccountRTReactive(KafkaBankAccount bankAccount) {
	log.info("Start createAccountRTReactive.....  "  +bankAccount);
	
	

	
	Flux<KafkaBankAccount> accountFlux=WebClient.create()
	.post()
    //.uri(END_POINT_URL)
	.uri(MONGO_END_POINT_URL+"/accounts")
    .body(Mono.just(bankAccount), KafkaBankAccount.class)
    .retrieve()
    .bodyToFlux(KafkaBankAccount.class)
    .timeout(Duration.ofMillis(100))
    .retryWhen(Retry.backoff(3, Duration.ofMillis(500))
            .jitter(0d)
            .doAfterRetry(retrySignal -> {
                log.info("Retried " + retrySignal.totalRetries());
                System.out.println("Retried " + retrySignal.totalRetries());
            })
            .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) 
                    -> new Exception("retry exhahusted....")))
   
    .doOnError( (msg) -> {
      // System.out.println("Message :: " + msg);
        // here update the DB
      //  dbRepository.save(...);
       // recover(bankAccount);
      //  reslut=false;
    })
    
    
    
    
    .onErrorResume(error ->  recover1(bankAccount));
    
    
    
    
    
  
              
    		
    		
    
   /* .onErrorResume(e -> {
        if (e instanceof Exception) {
            log.error("Failed to get myStuff, desired service not present");
        } else {
            log.error("Failed to get myStuff");
        }
        reslut=false;
        
         return null;
    });*/
    
    
    
    
        
  //  .subscribe();
	
	
	
	
	accountFlux.subscribe(account -> log.info("returned ::  "+account.toString()));
	log.info("reslut webflux ::"+ reslut);
	log.info("account webflux ::"+ accountFlux);
	return reslut;
}

	
private boolean is5xxServerError(Throwable throwable) {
    return throwable instanceof WebClientResponseException &&
            ((WebClientResponseException) throwable).getStatusCode().is5xxServerError();
}


private Mono<String> sayHelloFallback() {
    return Mono.just("Hello, Stranger");
}

	@Recover
    boolean recover(Exception e, KafkaBankAccount bankAccount) {
		//log.info("Start start Recover.....  "  +bankAccount);
		
		//HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
		//restTemplate.postForObject(MONGO_END_POINT_URL+"/accounts", request, KafkaBankAccount.class);
		return false;
	}
	
	
	KafkaBankAccount recover( KafkaBankAccount bankAccount) {
		log.info("Start start Recover.....  "  +bankAccount);
		
		HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
		return restTemplate.postForObject(MONGO_END_POINT_URL+"/accounts", request, KafkaBankAccount.class);
		
	}
	
	
	Mono<KafkaBankAccount> recover1( KafkaBankAccount bankAccount) {
		
		System.out.println("Recover 1......   " +bankAccount);
		return  Mono.just(bankAccount);
	}
	
	@Retryable( value = Exception.class, 
		      maxAttempts = 3, backoff = @Backoff(delay = 100))
	public boolean  createAccountMDB(KafkaBankAccount bankAccount) throws Exception{
		
		//log.debug("Start createAccountMDB.....  "  +bankAccount);
		HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
	    restTemplate.postForObject(MONGO_END_POINT_URL+"/accounts", request, KafkaBankAccount.class);
		return true;
	  }
	
	
	
	
	@Retryable( value = Exception.class, 
		      maxAttempts = 3, backoff = @Backoff(delay = 100))
	public boolean  sendToEdge(KafkaBankAccount bankAccount) throws Exception{
		
		//log.debug("Start createAccountMDB.....  "  +bankAccount);
		HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
	    restTemplate.postForObject(EDGE_END_POINT_URL, request, KafkaBankAccount.class);
		
	    return true;
	  }

	
public boolean  createAccountMDBReactive(KafkaBankAccount bankAccount) throws Exception{
		
		log.info("Start createAccountMDBReactive.....  "  +bankAccount);
	
	
	
	Flux<KafkaBankAccount> accountFlux = WebClient.create()
	          .post()
	          .uri(MONGO_END_POINT_URL+"/accounts")
	          .body(Mono.just(bankAccount), KafkaBankAccount.class)
	          .retrieve()
	          .bodyToFlux(KafkaBankAccount.class);
	   accountFlux.subscribe();
	   
	
	//accountFlux.subscribe(account -> log.info("returned ::  "+account.toString()));
	
   
   return true;
		
		
		
	  }
	
	
   public Object createAccountCB(KafkaBankAccount bankAccount) {
		
    	HttpEntity<KafkaBankAccount> request = new HttpEntity<>(bankAccount);
    	CircuitBreaker circuitBreaker = circuitBreakerFactory.create("circuitbreaker");
         circuitBreaker.run(() -> restTemplate.postForObject(END_POINT_URL, request, KafkaBankAccount.class),throwable -> handleFailOver(bankAccount));
    	return null;
    }


   
   
  
   
   
   
   public Object handleFailOver(KafkaBankAccount bankAccount) {
	
	   log.info("Start handleFailOver.....  "  +bankAccount);
	
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
	
	log.info("lenght for KafkaBankAccount[]="+response.getBody().length);
	
	
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
