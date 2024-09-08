/*
 * Copyright 2017-2018 the original author or authors.
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

package io.github.evaggelos99.r2dbc.h2.client;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.h2.command.CommandInterface;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Session;
import org.h2.engine.SessionRemote;
import org.h2.expression.ParameterInterface;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.value.Value;

import io.github.evaggelos99.r2dbc.h2.H2DatabaseExceptionFactory;
import io.github.evaggelos99.r2dbc.h2.util.Assert;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * An implementation of {@link Client} that wraps an H2 {@link Session}.
 */
public final class SessionClient implements Client {

	private final Logger logger = Loggers.getLogger(this.getClass());

	private final Collection<Binding> emptyBinding = Collections.singleton(Binding.EMPTY);

	private final Session session;

	private final boolean shutdownDatabaseOnClose;

	/**
	 * Creates a new instance.
	 *
	 * @param connectionInfo          the connection info to use
	 * @param shutdownDatabaseOnClose the flag that determines if the database will
	 *                                be closed on shutdown
	 * @throws NullPointerException if {@code connectionInfo} is {@code null}
	 */
	@SuppressWarnings("resource")
	public SessionClient(final ConnectionInfo connectionInfo, final boolean shutdownDatabaseOnClose) {
		Assert.requireNonNull(connectionInfo, "connectionInfo must not be null");

		this.session = new SessionRemote(connectionInfo).connectEmbeddedOrServer(false);
		this.shutdownDatabaseOnClose = shutdownDatabaseOnClose;
	}

	@Override
	public Mono<Void> close() {
		return Mono.defer(() -> {

			if (this.shutdownDatabaseOnClose) {
				try {
					final CommandInterface shutdown = this.session.prepareCommand("SHUTDOWN", 0);
					shutdown.executeUpdate(null);
				} catch (final DbException e) {
					return Mono.error(H2DatabaseExceptionFactory.convert(e));
				}
			}
			this.session.close();
			return Mono.empty();
		});
	}

	@Override
	public void disableAutoCommit() {
		this.session.setAutoCommit(false);
	}

	@Override
	public void enableAutoCommit() {
		this.session.setAutoCommit(true);
	}

	@Override
	public boolean inTransaction() {
		return !this.session.getAutoCommit();
	}

	@Override
	public Iterator<CommandInterface> prepareCommand(final String sql, final List<Binding> bindings) {
		Assert.requireNonNull(sql, "sql must not be null");
		Assert.requireNonNull(bindings, "bindings must not be null");

		if (!bindings.isEmpty()) {
			final Binding binding = bindings.get(bindings.size() - 1);
			if (binding.getParameters().isEmpty()) {
				throw new IllegalStateException("You got an unbound binder!");
			}
		}

		final Iterator<Binding> bindingIterator = bindings.isEmpty() ? emptyBinding.iterator() : bindings.iterator();

		return new Iterator<CommandInterface>() {

			@Override
			public boolean hasNext() {
				return bindingIterator.hasNext();
			}

			@Override
			public CommandInterface next() {
				final Binding binding = bindingIterator.next();

				try {
					final CommandInterface command = createCommand(sql, binding);
					logger.debug("Request:  {}", command);
					return command;
				} catch (final DbException e) {
					throw H2DatabaseExceptionFactory.convert(e);
				}
			}
		};
	}

	@Override
	public ResultInterface query(final CommandInterface command) {

		try {
			final ResultInterface result = command.executeQuery(Integer.MAX_VALUE, false);
			this.logger.debug("Response: {}", result);
			return result;
		} catch (final DbException e) {
			throw H2DatabaseExceptionFactory.convert(e);
		}
	}

	@Override
	public ResultWithGeneratedKeys update(final CommandInterface command, final Object generatedColumns) {
		return command.executeUpdate(generatedColumns);
	}

	/**
	 * Return back the current {@link Session} to the database.
	 */
	@Override
	public Session getSession() {
		return this.session;
	}

	private CommandInterface createCommand(final String sql, final Binding binding) {
		try {
			final CommandInterface command = this.session.prepareCommand(sql, Integer.MAX_VALUE);

			final List<? extends ParameterInterface> parameters = command.getParameters();
			for (final Map.Entry<Integer, Value> entry : binding.getParameters().entrySet()) {
				parameters.get(entry.getKey()).setValue(entry.getValue(), false);
			}

			return command;
		} catch (final DbException e) {
			throw H2DatabaseExceptionFactory.convert(e);
		}
	}
}
