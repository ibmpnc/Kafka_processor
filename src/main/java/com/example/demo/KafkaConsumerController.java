package com.example.demo;

import java.io.StringReader;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.model.KafkaBankAccount;
import com.example.demo.model.KafkaXMLBankAccount;
import com.example.demo.service.BankAccountService;
import com.example.demo.service.ConsumerCreator;
import com.example.demo.service.IKafkaConstants;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class KafkaConsumerController {
	
	 private KafkaStreams bankAccountStream;
	    @Autowired
		 BankAccountService bankAccountService;
	    
	    boolean exitflag=false;
	    long lastCommitedOffset=0;
		long lastOffset=0;
		boolean startFlag=false;
		long processedCount=0;
		long totalProcessed=0;
		long streamProcessedCount=0;
		boolean startStreamFlag=true;
		boolean exitFlag=false;
		long errorCount=0;
		Timer timer;
		public static final int TIME_THRESHHOLD=120;
		public static final int ERROR_COUNT_THRESHHOLD=20;
		long startErrorOffset;
		
		 @Value("${app.topic.example}")
		    private String topic;
		    
		    @Value("${xmlapp.topic.example}")
		    private String xmltopic;
		    
		    @Value("${spring.kafka.bootstrap-servers}")
		    private String bootstrapServers;
		    
		    Consumer<Long, String> consumer ;
		    
		    

		    @RequestMapping("/runConsumer/")
		     void runConsumer() {
		    	try {
		    	consumer = ConsumerCreator.createConsumer();
		    	exitFlag=false;
		    	while (true) {
					
					if(exitFlag) {
						break;
					}
		    	}
		   
		    	}finally {
					consumer.close();
		    	}
		    
		    }
	    
	  
	   public  void resume() {
			
		   log.debug("resuming.......");
	    	exitFlag=false;
	    	startFlag=true;
			
        
			while (true) {
				
				if(exitFlag) {
					break;
				}
				
				if(this.startFlag ) {
				final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				
				this.process(consumerRecords);
				
			
				
			}else {
				break;
			}
			}
			
			
			
	
		}
		
	
	    
	    
	   
	  
	    
	    @RequestMapping("/stop/")
	    public void stop() {
	    	log.info("stop  ");
	    	log.debug("Last commited offset ="+ this.lastCommitedOffset);
	    	log.debug("Porcessed Count="+ this.processedCount);
	    	totalProcessed=totalProcessed+this.processedCount;
	    	//log.debug("totalProcessed ="+ this.totalProcessed);
	    	startFlag=false;
	    	
	    }
	    
	    @RequestMapping("/exit/")
	    public void exit() {
	    	log.info("Exiting the Consumer ");
	    	if(!startFlag) {
	    	    exitFlag=true;
	    	}
	    	
	    }
	    
	   
	    
	    @GetMapping("/start")
		 public void start(@RequestParam(required = false) String offset, @RequestParam(required = false) String timeOffset)   {
	    	
	    	log.debug("Start startFlag="+startFlag  + "offset= "+offset);
	    	log.info("Start ");
	    	if(!startFlag) {
	    		errorCount=0;
            	timerExpired=false;
	    	if( offset==null && timeOffset==null ) {
	    		
	    		this.resume();
	    	}else {
	    		
	    		if(offset!=null) {
	    		rewindToOffset(Long.parseLong(offset));
	    		}
	    		else {
	    			this.rewindToTime(Long.parseLong(timeOffset));
	    		}
	    			
	    			
	    	}
	    	
	    	
	    	
	    	
	    	
	    	}
	    }
	    
	   
	    
	    @GetMapping("/restart")
		 public void restart()   {
	    	
	    	log.debug("Re Start startFlag="+startFlag  + " offset= "+startErrorOffset);
	    	log.info("Re Start ");
	    	if(!startFlag && startErrorOffset !=0) {
	    		errorCount=0;
           	timerExpired=false;
	    		
	    		rewindToOffset(startErrorOffset);
	    	
	    	}
	    }
	  
	 public void rewindToOffset(long offset)   {
		 
		 log.debug("Start from offset ="  +offset);
		
	    
	    boolean exitFlag = false;
	    startFlag=true;
	    
	    consumer.seek(new TopicPartition(IKafkaConstants.TOPIC_NAME, 0), offset);
	   
	    while (true) {
	    	
	    	if(exitFlag) {
				break;
			}
	    	
	    	
	    	
	    	if(this.startFlag ) {
	        ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

	       
	           // consumer.seek(new TopicPartition(IKafkaConstants.TOPIC_NAME, 0), offset);
	           
	       

	        this.process(consumerRecords);
	        
	        
	        
	    	}
	    	else {
	    		break;
	    	}
	    }

	 }  
	 

	 
	 
public void rewindToTime(long minutes)   {
		 
		 log.debug("Start from  ="  +minutes  +"  minutes ago " );
		
	    
	    boolean exitFlag = false;
	    startFlag=true;
	    Map <TopicPartition,Long>query= new HashMap();
	   
	    
	    query.put(
                new TopicPartition(IKafkaConstants.TOPIC_NAME, 0),
                Instant.now().minus(minutes, ChronoUnit.MINUTES).toEpochMilli());

        Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(query);
	    
        result.entrySet()
        .stream()
        .forEach(entry -> consumer.seek(entry.getKey(), entry.getValue().offset()));

	    
	    
	   
	    while (true) {
	    	
	    	if(exitFlag) {
				break;
			}
	    	
	    	
	    	
	    	if(this.startFlag ) {
	        ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

	        this.process(consumerRecords);
	        
	    	}
	    	else {
	    		break;
	    	}
	    }

	 }  



boolean errorFlag=false;

public void  process(ConsumerRecords<Long, String> consumerRecords) {
	
	
	 consumerRecords.forEach(record -> {
			
			log.debug("Record offset " + record.offset()+" Record value " + record.value());
			log.info(" " + record.value());
			log.debug("recordTimeStap " + new Date (record.timestamp()));
			lastOffset=record.offset();
		
			
			if(lastCommitedOffset==0)
				lastCommitedOffset=lastOffset-1;
		
			JAXBContext jaxbContext;
         try
         {
             jaxbContext = JAXBContext.newInstance(KafkaXMLBankAccount.class);              
          
             Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
          
             KafkaXMLBankAccount xmlaccount = (KafkaXMLBankAccount) jaxbUnmarshaller.unmarshal(new StringReader(record.value()));
            // log.debug(xmlaccount.toString());
             KafkaBankAccount account= new KafkaBankAccount(xmlaccount.getId(),xmlaccount.getBalance(),xmlaccount.getName(),xmlaccount.getAddress(),xmlaccount.getAccountType());
            boolean result= bankAccountService.createAccountMDB(account);
            //boolean result= bankAccountService.sendToEdge(account)   ;
           
            log.debug("Reslult="+result+"   errorCount="+errorCount+"  timerExpired="+timerExpired);
           
            
            if(!result && errorCount==0) {
            	log.error("Error    "+account );
            	//errorCount++;
            	startErrorOffset=record.offset();
            	 log.info(  "errorCount="+errorCount);
            	 errorFlag=true;
            	startTimer();
            }
            
           /* else if(!result &&  errorCount>=1 && !this.timerExpired) {
            	log.error("Error    "+account );
            	errorCount++;
            	errorFlag=true;
            }*/
            
            
           else if (!result &&  this.errorCount>=this.ERROR_COUNT_THRESHHOLD){
            	log.error("Error    "+account );
            	log.info(  "errorCount="+errorCount);
            	log.debug("Error started time ="+ new Date(System.currentTimeMillis()-this.TIME_THRESHHOLD));
            	log.debug("startErrorOffset "+ startErrorOffset);
            	this.stop();
            	
            }
            
            
          /* else if (!result &&  errorCount>=1 && this.timerExpired){
          	log.error("Error    "+account );
           	log.debug("Error started time ="+ new Date(System.currentTimeMillis()-this.TIME_THRESHHOLD));
            	log.debug("startErrorOffset "+ startErrorOffset);
            	this.stop();
            	
            }*/
         else if(result) {
            	errorCount=0;
            	timerExpired=false;
            	errorFlag=false;
            }
         else {
        	 if(errorCount>=1) {
        		 log.info(  "errorCount="+errorCount);
        		 log.error("Error    "+account );
        		 errorFlag=true;
        		 
        	 }
        	// errorCount++;
         }
            // consumer.commitAsync();
             lastCommitedOffset=record.offset();
             processedCount++;
             if(errorFlag)
            	 errorCount++;
         
         }
         catch (Exception e) 
         {
             e.printStackTrace();
         }
        
         try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
}


boolean timerExpired=false;
public void  startTimer() {
	timer = new Timer();
    timer.schedule(new RemindTask(),this.TIME_THRESHHOLD*1000);
}


class RemindTask extends TimerTask {
    public void run() {
        timer.cancel(); //Terminate the timer thread
        timerExpired=true;
    }
    
    
    
}


public static void main(String args[]) {
	
	KafkaConsumerController kafkaConsumerController = new KafkaConsumerController();
	kafkaConsumerController.startTimer();
    System.out.println("Task scheduled.");
    
    while(!kafkaConsumerController.timerExpired) {
    	System.out.println(""+kafkaConsumerController.timerExpired);
    }
    System.out.println("Time's up!");
    
}
}
