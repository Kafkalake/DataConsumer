package com.hooniegit.DataConsumer.Kafka;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter
@NoArgsConstructor
@AllArgsConstructor
public class StatusData {

    private int id;
    private String value;
    private String timestamp;

}
