package com.hooniegit.DataConsumer;

import com.hooniegit.SpringInitializer.IniConfigApplicationContextInitializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class DataConsumerApplication {

	public static void main(String[] args) {
		new SpringApplicationBuilder(DataConsumerApplication.class)
				.initializers(new IniConfigApplicationContextInitializer())
				.run(args);
	}

}
