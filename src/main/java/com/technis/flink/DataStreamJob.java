package com.technis.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// set up execution environment
		final StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();

		env
				.fromSequence(1, 10)
				.filter(value -> value % 2 == 0)
				.print();
		env.execute("Even Numbers Filter");
	}
}