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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
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
 *
 * 이에 대한 올바른 hook는 open() 메소드이다.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	/**
	 * ValueState는 래핑 클래스입니다. 자바 표준 라이브러리의 AtomicReference, AtomicLong과 비슷하다.
	 * Contents와 상호작용하기 위해 3가지 메소드가 제공된다.
	 * - set : 상태 업데이트
	 * - get : 현재 값 조회
	 * - clear : Contents
	 * 만약 특정 키의 상태가 비어있다면(어플리케이션이 시작한 뒤나 ValueState#clear가 호출된 후)
	 * ValueState#value는 null을 반환한다.
	 *
	 * ValueState#value에 의해 반환된 개체에 대한 수정사항은 시스템에서 인식되지 않을 수 있으므로
	 * 모든 변경사항은 ValueState#update를 사용하여 수행해야 한다.
	 *
	 * Otherwise, fault tolerance is managed automatically by Flink under the hood,
	 * and so you can interact with it like with any standard variable.
     *
	 */
	private transient ValueState<Boolean> flagState;

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN);
		flagState = getRuntimeContext().getState(flagDescriptor);
	}


	/**
	 * 모든 transaction에서 fraud detector는 해당 계정의 flat state를 확인한다.
	 *
	 * ValueState는 현재 key에 한정되어 있다는 것을 기억해야 한다.
	 * 만약 flag가 null이 아니라면, 해당 account의 이전 transaction이 작았다는 뜻이고 큰 transaction이 그 다음으로 온다면
	 * detector는 fraud alert를 출력할 것이다.
	 *
	 * 확인이 끝난 뒤, flag state는 무조건 clear된다.
	 * Either the current transaction caused a fraud alert,
	 * and the pattern is over,
	 * or the current transaction did not cause an alert,
	 * and the pattern is broken and needs to be restarted.
	 *
	 * 마지막으로, transaction의 크기가 작은지 확인한다. 만약 작다면 flag는 set되고 다음 event에서 확인된다.
	 *
	 * ValueState<Boolean>이 3가지 상태를 가지고 있다는걸 기억해야 한다.
	 * 	- unset(null)
	 *	- true
	 * 	- false
	 * 왜냐하면 ValueState는 nullable하기 때문이다.
	 *
	 * 해당 예시에서는 unset과 true만 확인한다.
	 */
	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		// Get the current state for the current key
		Boolean lastTransactionWasSmall = flagState.value();

		// Check if the flag is set
		// true, false가 아닌 set, unset으로 판단하는 것 같다.
		if (lastTransactionWasSmall != null) {
			if (transaction.getAmount() > LARGE_AMOUNT) {
				// Output an alert downstream
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());

				collector.collect(alert);
			}

			// Clean up our state
			flagState.clear();
		}

		if (transaction.getAmount() < SMALL_AMOUNT) {
			// Set the flag to true
			flagState.update(true);
		}
	}
}
