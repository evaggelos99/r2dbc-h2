/*
 * Copyright 2019 the original author or authors.
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

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

final class H2ConnectionFactoryInMemoryTest {

	@Test
	void shouldCreateInMemoryDatabase() {

		final CloseableConnectionFactory connectionFactory = H2ConnectionFactory.inMemory(UUID.randomUUID().toString());

		connectionFactory.create().flatMapMany(H2Connection::close).as(StepVerifier::create).verifyComplete();
	}

	@Test
	void retainsStateAfterRunningCommand() {

		final CloseableConnectionFactory connectionFactory = H2ConnectionFactory.inMemory(UUID.randomUUID().toString());

		runCommand(connectionFactory, "CREATE TABLE lego (id INT);");
		runCommand(connectionFactory, "INSERT INTO lego VALUES(42);");
	}

	@Test
	void databaseClosedAfterFactoryClose() {

		final String database = UUID.randomUUID().toString();
		final CloseableConnectionFactory connectionFactory = H2ConnectionFactory.inMemory(database);

		runCommand(connectionFactory, "CREATE TABLE lego (id INT);");

		connectionFactory.close().as(StepVerifier::create).verifyComplete();

		final CloseableConnectionFactory nextInstance = H2ConnectionFactory.inMemory(database);
		runCommand(nextInstance, "CREATE TABLE lego (id INT);");
	}

	@Test
	void closedDatabaseFailsToCreateConnections() {

		final String database = UUID.randomUUID().toString();
		final CloseableConnectionFactory connectionFactory = H2ConnectionFactory.inMemory(database);

		runCommand(connectionFactory, "CREATE TABLE lego (id INT);");

		connectionFactory.close().as(StepVerifier::create).verifyComplete();
		connectionFactory.create().as(StepVerifier::create).verifyError(R2dbcNonTransientResourceException.class);
	}

	@Test
	void closedDatabaseShutsDownClientSessions() {

		final String database = UUID.randomUUID().toString();
		final CloseableConnectionFactory connectionFactory = H2ConnectionFactory.inMemory(database);

		final H2Connection userSession = connectionFactory.create().block();

		connectionFactory.close().as(StepVerifier::create).verifyComplete();

		userSession.createStatement("CREATE TABLE lego (id INT);").execute().as(StepVerifier::create)
				.verifyError(R2dbcNonTransientResourceException.class);
	}

	static void runCommand(final ConnectionFactory connectionFactory, final String sql) {

		Mono.from(connectionFactory.create()).flatMapMany(it -> Flux.from(it.createStatement(sql).execute()).flatMap(Result::getRowsUpdated).thenMany(it.close())).as(StepVerifier::create).verifyComplete();
	}
}
