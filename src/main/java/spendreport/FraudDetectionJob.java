/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * Skeleton code for the datastream walkthrough
 *
 * Application의 Data flow를 정의
 */
public class FraudDetectionJob {
	public static void main(String[] args) throws Exception {

		// Job 실행, source 생성, job trigger가 실행되는 환경에 대한 설정
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/* 해당 실습에서는 무한히 생성되는 credit card transaction stream이 제공된다.
		  각 transaction은 accountId, timestamp(transaction이 발생한), US$ amount가 포함되어 있다.
		   여기서 name()은 단순히 디버깅에 사용되는 구분용 이름
		*/
		DataStream<Transaction> transactions = env
			.addSource(new TransactionSource())
			.name("transactions");

		/* Transaction stream은 대규모의 사용자로부터 발생하는 transaction을 포함하고있다.
			따라서 다중 사기 탐지 task가 병렬적으로 수행되어야 한다.

			사기는 계정별로 발생하므로 동일한 계정에 대한 모든 거래가 병렬로 수행되는 사기 탐지 작업에서 처리되어야 한다.

			동일한 물리적 작업이 특정 키에 대한 모든 레코드를 처리하도록 하려면
			DataStream#keyBy를 통해 stream을 나눌 수 있다.

			process() 를 호출하여 stream 내부에서 분할된 요소에 적용할 함수를 추가할 수 있다.

			연산자는 keyBy아래에 선언하는것이 일반적이다.
		 */

		DataStream<Alert> alerts = transactions
			.keyBy(Transaction::getAccountId)
			.process(new FraudDetector())
			.name("fraud-detector");


		/*
			Sink는 DataStream을 외부 시스템으로 출력한다.
			- 예를 들어 Apache Kafka, Cassandra, AWS Kinesis 등이 있다.

			AlertSink는 각 Alert들을  영속적인 저장소에 저장하는 대신 INFO level log로 기록한다.


		*/
		alerts
			.addSink(new AlertSink())
			.name("send-alerts");

		env.execute("Fraud Detection");
	}
}
