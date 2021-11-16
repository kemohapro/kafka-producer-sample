package com.kemoha.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kemoha.domain.LibraryEvent;
import com.kemoha.domain.LibraryEventType;
import com.kemoha.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LibraryEventsController {
	
	
	//Asynchronous
	@Autowired
	private LibraryEventProducer libraryEventProducer;
	
	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
		
		log.info("Beginning of Processing");
		
		libraryEventProducer.sendLibraryEvent(libraryEvent);
		
		log.info("Middle of Processing");
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
		
	}
	
	//Asynchronous
		@PostMapping("/v3/libraryevent")
		public ResponseEntity<LibraryEvent> sendLibraryEvent_usingProducer(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
			
			log.info("Beginning of Processing");
			libraryEvent.setEventType(LibraryEventType.NEW);
			libraryEventProducer.sendLibraryEvent_usingProducer(libraryEvent);
			
			log.info("Middle of Processing");
			return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
			
		}
	
	//Synchronous
	@PostMapping("/v2/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent_Synchronous(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
		
		log.info("Beginning of Processing");
		
		SendResult<Integer, String> sendResult = libraryEventProducer.postLibraryEvent_Synchronous(libraryEvent);
		
		log.info("Send Result {}", sendResult.toString());
		
		log.info("Middle of Processing");
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
		
	}
	
	
	@PutMapping("/v4/libraryevent")
	public ResponseEntity<?> putLibraryEvent_usingProducer(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
		
		log.info("Beginning of Processing");
		
		if(libraryEvent.getLibraryEventId()==null) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please send the Library Event ID");
		}
		
		libraryEvent.setEventType(LibraryEventType.UPDATE);
		libraryEventProducer.sendLibraryEvent_usingProducer(libraryEvent);
		
		log.info("Middle of Processing");
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
		
	}


}
