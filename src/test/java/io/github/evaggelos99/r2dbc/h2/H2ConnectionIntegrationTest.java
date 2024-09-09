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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.UUID;

import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.junit.jupiter.api.Test;

import io.github.evaggelos99.r2dbc.h2.client.SessionClient;
import io.github.evaggelos99.r2dbc.h2.codecs.DefaultCodecs;

final class H2ConnectionIntegrationTest {

	@Test
	void getMetadata() {

		final ConnectionInfo configuration = new ConnectionInfo(
				"jdbc:h2:mem:" + UUID.randomUUID().toString() + ";USER=sa;PASSWORD=sa;", new Properties(), null, null);
		final SessionClient sessionClient = new SessionClient(configuration, false);

		final H2Connection connection = new H2Connection(sessionClient, new DefaultCodecs(sessionClient));
		final H2ConnectionMetadata metadata = connection.getMetadata();

		assertThat(metadata.getDatabaseVersion()).isEqualTo(Constants.VERSION);
	}
}
