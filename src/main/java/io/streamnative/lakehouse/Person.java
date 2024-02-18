package io.streamnative.lakehouse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person {
    private String name;
    private Integer age;
    private String address;
    private Boolean gender;
    private Double score;
    private Long ba;
}
