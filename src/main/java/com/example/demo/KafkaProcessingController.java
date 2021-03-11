package com.example.demo;

import java.io.StringReader;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.model.KafkaBankAccount;
import com.example.demo.model.KafkaXMLBankAccount;
import com.example.demo.serde.JsonDeserializer;
import com.example.demo.serde.JsonSerializer;
import com.example.demo.service.BankAccountService;

@RestController
public class KafkaProcessingController {

   
    private KafkaStreams bankAccountStream;
    @Autowired
	 BankAccountService bankAccountService;
    
    public static final long  WAIT_TIME_TO_CHECK_EDGE=120000;
   
    
    @Value("${app.topic.example}")
    private String topic;
    
    @Value("${xmlapp.topic.example}")
    private String xmltopic;
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

  

    @RequestMapping("/stop/")
    public void stopStreams() {
      
        bankAccountStream.close();
    }
    
    
    
    
    @RequestMapping("/startProcessingBankAccounts/")
    public void startProcessingBankAccounts() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bankAccountStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
       

        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, BankAccountSerde.class.getName());        
        
        
        final StreamsBuilder builder = new StreamsBuilder();

       
        
        KStream<String, KafkaBankAccount> source = builder.stream(topic);
        
        
        source.foreach((k,v)->{
        	
            System.out.println("Key="+ k + "   value="+ v);
        	bankAccountService.createAccount(v);
            //bankAccountService.createAccountCB(v);
            
        	
        });
       
        final Topology topology = builder.build();
        bankAccountStream = new KafkaStreams(topology, props);
        bankAccountStream.start();

    }
    
    
    boolean edgeServerUp=true;
    long timer=System.currentTimeMillis();
    boolean mongodbisHasRecs=true;
    
    @RequestMapping("/startProcessingxmlBankAccountsnew/")
    public void startProcessingxmlBankAccounts_new() {
    	
    	Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "xmlbankAccountStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());      
        
        
        final StreamsBuilder builder = new StreamsBuilder();

       
        
        KStream<String, String> source = builder.stream(xmltopic);
        
      
       
        
        source.foreach((k,v)->{
        	
            System.out.println("Key="+ k + "   value="+ v);
           
        
            JAXBContext jaxbContext;
            try
            {
                jaxbContext = JAXBContext.newInstance(KafkaXMLBankAccount.class);              
             
                Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
             
                KafkaXMLBankAccount xmlaccount = (KafkaXMLBankAccount) jaxbUnmarshaller.unmarshal(new StringReader(v));
                 
                System.out.println(xmlaccount);
                KafkaBankAccount account= new KafkaBankAccount(xmlaccount.getBalance(),xmlaccount.getName(),xmlaccount.getAddress(),xmlaccount.getAccountType());
                
               // bankAccountService.createAccountCB(account);
                //bankAccountService.createAccount(account);
              
                if(edgeServerUp || (System.currentTimeMillis()-timer > WAIT_TIME_TO_CHECK_EDGE)) {
               	
                boolean result=  bankAccountService.createAccountRT(account);
                	
                	if(result && !edgeServerUp  && mongodbisHasRecs) {
                		bankAccountService.sendAllFromMongodb();
                		mongodbisHasRecs=false;
                	}
                	
                	if(!result ) {
                		edgeServerUp=false;
                		timer=System.currentTimeMillis();
                	}
                
                }else {
                	bankAccountService.createAccountMDB(account);
                	if(!mongodbisHasRecs)
                		mongodbisHasRecs=true;
                }
                
             
            }
            catch (Exception e) 
            {
                e.printStackTrace();
            }
            
        	
        });
       
        final Topology topology = builder.build();
        bankAccountStream = new KafkaStreams(topology, props);
        bankAccountStream.start();

    }
    
    
    
    
    
    @RequestMapping("/startProcessingxmlBankAccounts/")
    public void startProcessingxmlBankAccounts() {
    	
    	Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "xmlbankAccountStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());      
        
        
        final StreamsBuilder builder = new StreamsBuilder();

       
        
        KStream<String, String> source = builder.stream(xmltopic);
        
      
        boolean edgeServerUp=true;
        
        source.foreach((k,v)->{
        	
            System.out.println("Key="+ k + "   value="+ v);
           
        
            JAXBContext jaxbContext;
            try
            {
                jaxbContext = JAXBContext.newInstance(KafkaXMLBankAccount.class);              
             
                Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
             
                KafkaXMLBankAccount xmlaccount = (KafkaXMLBankAccount) jaxbUnmarshaller.unmarshal(new StringReader(v));
                 
                System.out.println(xmlaccount);
                KafkaBankAccount account= new KafkaBankAccount(xmlaccount.getBalance(),xmlaccount.getName(),xmlaccount.getAddress(),xmlaccount.getAccountType());
                
               // bankAccountService.createAccountCB(account);
                //bankAccountService.createAccount(account);
              
               
                boolean result=	  bankAccountService.createAccountRT(account);
                	
                
              
              
              System.out.println("Successul="+ result);
            }
            catch (Exception e) 
            {
                e.printStackTrace();
            }
            
        	
        });
       
        final Topology topology = builder.build();
        bankAccountStream = new KafkaStreams(topology, props);
        bankAccountStream.start();

    }
    
    
    
    static public final class BankAccountSerde extends WrapperSerde<KafkaBankAccount> {
        public BankAccountSerde() {
            super(new JsonSerializer<KafkaBankAccount>(), new JsonDeserializer<KafkaBankAccount>(KafkaBankAccount.class));
        }
    }
    
    
    
   

}

