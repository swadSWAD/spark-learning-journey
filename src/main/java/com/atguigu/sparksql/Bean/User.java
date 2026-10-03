package com.atguigu.sparksql.Bean;

import lombok.Data;
import java.io.Serializable;

@Data
public class User implements Serializable {
    public Long age;
    public String name;

    public User() {
    }

    public User(Long age, String name) {
        this.age = age;
        this.name = name;
    }
}