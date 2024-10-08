/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.evaggelos99.r2dbc.h2;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.h2.command.CommandInterface;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.github.evaggelos99.r2dbc.h2.client.Client;
import io.github.evaggelos99.r2dbc.h2.codecs.MockCodecs;
import io.r2dbc.spi.R2dbcBadGrammarException;
import reactor.test.StepVerifier;

final class H2BatchTest {

	private final Client client = mock(Client.class, RETURNS_SMART_NULLS);

	@Test
	void addNoSql() {
		assertThatIllegalArgumentException().isThrownBy(() -> new H2Batch(this.client, MockCodecs.empty()).add(null))
				.withMessage("sql must not be null");
	}

	@Disabled("Not yet implemented")
	@Test
	void addWithParameter() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new H2Batch(this.client, MockCodecs.empty()).add("test-query-$1")).withMessage(
						"Statement 'test-query-$1' is not supported.  This is often due to the presence of parameters.");
	}

	@Test
	void constructorNoClient() {
		assertThatIllegalArgumentException().isThrownBy(() -> new H2Batch(null, MockCodecs.empty()))
				.withMessage("client must not be null");
	}

	@Test
	void constructorNoCodecs() {
		assertThatIllegalArgumentException().isThrownBy(() -> new H2Batch(this.client, null))
				.withMessage("codecs must not be null");
	}

	@Test
	void execute() {
		final CommandInterface command1 = mock(CommandInterface.class);
		final CommandInterface command2 = mock(CommandInterface.class);
		when(this.client.prepareCommand("select test-query-1", Collections.emptyList()))
				.thenReturn(Collections.singleton(command1).iterator());
		when(this.client.prepareCommand("select test-query-2", Collections.emptyList()))
				.thenReturn(Collections.singleton(command1).iterator());
		when(command1.isQuery()).thenReturn(true);
		when(command2.isQuery()).thenReturn(true);
		when(this.client.query(command1)).thenReturn(new LocalResult());
		when(this.client.query(command2)).thenReturn(new LocalResult());

		new H2Batch(this.client, MockCodecs.empty()).add("select test-query-1").add("select test-query-2").execute()
				.as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@Test
	void executeErrorResponse() {
		final CommandInterface command = mock(CommandInterface.class);
		when(this.client.prepareCommand("select test-query", Collections.emptyList()))
				.thenReturn(Collections.singleton(command).iterator());
		when(command.isQuery()).thenReturn(true);
		when(this.client.query(command)).thenThrow(DbException.getSyntaxError("bad statement", 999));

		new H2Batch(this.client, MockCodecs.empty()).add("select test-query").execute().as(StepVerifier::create)
				.verifyError(R2dbcBadGrammarException.class);
	}

}
