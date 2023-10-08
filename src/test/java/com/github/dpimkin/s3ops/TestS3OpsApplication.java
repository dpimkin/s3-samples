package com.github.dpimkin.s3ops;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.TestConfiguration;

@TestConfiguration(proxyBeanMethods = false)
public class TestS3OpsApplication {

	public static void main(String[] args) {
		SpringApplication.from(S3OpsApplication::main).with(TestS3OpsApplication.class).run(args);
	}

}
