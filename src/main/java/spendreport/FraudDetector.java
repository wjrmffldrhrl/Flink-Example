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

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * Skeleton code for implementing a fraud detector.
 *
 * 비정상 거래 행위를 감지하는 Business logic
 *
 * KeyedProcessFunction의 구현체이다.
 * processElement는 모든 transaction event를 호출한다.
 *
 *
 * # Business Logic
 * ## Version 1
 * 모든 계정에서 작은 transaction 이후 즉시 큰 transaction이 따라온다면 경고를 울려야 한다.
 *
 * 여기서 작은 값은 $1.00 이하, 큰 값은 $500 이상이다.
 *
 * 이것을 처리하기 위해서는 fraud detector는 반드시 이벤트들간의 정보를 기억해야 한다.
 * 큰 transaction은 이전 transaction이 작은 경우에만 사기이다.
 * Event들의 정보를 기억하는것은 state를 필요로 하며, 이것이 KeyedProcessFunction을 선택한 이유이다.
 *
 * KeyedProcessFunction는 상태와 시간 모두에 대한 세밀한 제어를 제공하므로 더 복잡한 요구 사항으로 알고리즘을 발전시킬 수 있다.
 *
 * 간단한 구현 방법은 작은 transaction이 처리될 때 boolean flag를 set하는 것이다.
 * 큰 transaction이 통과할 때 간단하게 해당 계정에 대해서 flag를 확인해볼 수 있다.
 *
 * 그러나, 간단한 변수값을 통한 flag 구현으로는 FraudDetector class는 동작하지 않는다.
 * Flink는 다수의 계정에서 발생하는 transaction들을 같은 FraudDetector instance로 처리한다.
 * 만약 A, B 계정이 같은 instnace로 전달되면,
 * A 계정의 transaction이 flag를 set했을 때 B 계정이 flag를 off하여 잘못된 경고를 출력할 수 있다.
 *
 * 반드시, Map과 같은 구조를 통해 개개인의 key를 유지해야 한다.
 * 그런데 단순한 맴버 변수는 fault-tolerant가 보장되지 않고 실패시 모든 정보를 잃는다.
 * 이러한 이유로 fraud detector는 실패 복구를 위해 재시작 시 alerts를 놓칠 수 있다.
 *
 * 이 문제를 해결하기 위해 Flink는 일반 맴버 변수만큼 사용하기 쉬운 실패 용인 상태(내결함성 상태 fault-tolerant)를 위한 기본 요소를 제공한다.
 *
 * 가장 기본적인 상태 타입은 ValueState이다.
 * 이 데이터 타입은 내결함성을 더하기 위해 변수를 래핑한다.
 *
 * ValueState는 키 상태에서 유례하였으므로 키가 있는 데이터 처리에서 적용되는 연산자에서만 사용할 수 있다
 * - DataStream#keyBy 다음에 바로 오는 연산자들
 *
 * 연산자의 키 상태는 자동으로 현재 처리중인 레코드의 키로 범위가 지정된다.
 *
 * 여기서는 현재 transaction의 accountId가 key이며 FraudDector는 각각의 계정마다 독립적인 상태를 유지한다.
 *
 * ValueState는 Flink가 변수를 관리하는 방법에 대한 메타데이터가 포함된 ValueStateDescriptor를 사용해 만들어진다.
 * 이 상태는 함수가 데이터 처리를 시작하기 전에 등록될 것이다.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		Alert alert = new Alert();
		alert.setId(transaction.getAccountId());

		collector.collect(alert);
	}
}
