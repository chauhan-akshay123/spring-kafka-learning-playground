package com.akshay.learnKafka.user_service.dto.request;

import lombok.Data;

@Data
public class CreateUserRequestDTO {

    private Long id;
    private String name;
    private String email;
}
