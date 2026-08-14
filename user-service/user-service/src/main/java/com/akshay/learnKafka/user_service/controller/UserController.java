package com.akshay.learnKafka.user_service.controller;

import com.akshay.learnKafka.user_service.dto.request.CreateUserRequestDTO;
import com.akshay.learnKafka.user_service.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
@RequestMapping("/users")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @Value("${kafka.topic.user-random-topic}")
    private String KAFKA_RANDOM_USER_TOPIC;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/{message}")
    public ResponseEntity<String> sendMessage(@PathVariable String message) {

        for(int i=0; i<100; i++) {
            kafkaTemplate.send(KAFKA_RANDOM_USER_TOPIC, ""+i%3, message+i);
        }

        return ResponseEntity.ok("Message queued");
    }

    @PostMapping
    public ResponseEntity<String> createUser(@RequestBody CreateUserRequestDTO request) {
        userService.createUser(request);
        return ResponseEntity.ok("User created successfully");
    }
}
