package com.akshay.learnKafka.user_service.service;

import com.akshay.learnKafka.user_service.dto.request.CreateUserRequestDTO;
import com.akshay.learnKafka.user_service.entity.User;
import com.akshay.learnKafka.user_service.event.UserCreatedEvent;
import com.akshay.learnKafka.user_service.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserService {

    @Value("${kafka.topic.user-created-topic}")
    private String KAFKA_USER_CREATED_TOPIC;

    private final UserRepository userRepository;
    private final ModelMapper modelMapper;
    private final KafkaTemplate<Long, UserCreatedEvent> kafkaTemplate;

    public void createUser(CreateUserRequestDTO request) {
      User user = modelMapper.map(request,  User.class);
      User savedUser = userRepository.save(user);

      UserCreatedEvent userCreatedEvent = modelMapper.map(savedUser, UserCreatedEvent.class);
      kafkaTemplate.send(KAFKA_USER_CREATED_TOPIC, userCreatedEvent);
    }
}
