/*
 * Copyright 2017-2019 the original author or authors.
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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.h2.command.Command;
import org.h2.command.CommandInterface;
import org.h2.engine.Session;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;

import io.github.evaggelos99.r2dbc.h2.util.Assert;
import reactor.core.publisher.Mono;

/**
 * An abstraction that wraps interaction with the H2 Database APIs.
 */
public interface Client {

	/**
	 * Release any resources held by the {@link Client}.
	 *
	 * @return a {@link Mono} that termination is complete
	 */
	Mono<Void> close();

	/**
	 * Disable auto-commit. Typically used at the beginning of a transaction.
	 *
	 */
	void disableAutoCommit();

	/**
	 * Enables auto-commit. Typically used at the end of a transaction, either
	 * success or failure.
	 *
	 */
	void enableAutoCommit();

	/**
	 * Execute a command, discarding any results.
	 *
	 * @param sql the SQL of the command
	 * @throws NullPointerException if {@code sql} is {@code null}
	 */
	default void execute(final String sql) {
		Assert.requireNonNull(sql, "sql must not be null");

		final Iterator<CommandInterface> iterator = prepareCommand(sql, Collections.emptyList());

		while (iterator.hasNext()) {
			final CommandInterface command = iterator.next();
			update(command, false);

			if (command instanceof Command) {
				((Command) command).setCanReuse(true);
			}
		}
	}

	/**
	 * Whether the {@link Client} is currently in a transaction.
	 *
	 * @return {@code true} if the {@link Client} is currently in a transaction,
	 *         {@code false} otherwise.
	 */
	boolean inTransaction();

	/**
	 * Transform a SQL statement and a set of {@link Binding}s into a
	 * {@link CommandInterface}.
	 *
	 * @param sql      to either query or update
	 * @param bindings the parameter bindings to use
	 * @return {@link CommandInterface} to be flat mapped over
	 */
	Iterator<CommandInterface> prepareCommand(String sql, List<Binding> bindings);

	/**
	 * Execute a query.
	 *
	 * @param command the {@link CommandInterface} to query
	 * @return the result of the query
	 * @throws NullPointerException if {@code sql} or {@code bindings} is
	 *                              {@code null}
	 */
	ResultInterface query(CommandInterface command);

	/**
	 * Execute an update.
	 *
	 * @param command          the {@link CommandInterface} to update
	 * @param generatedColumns the parameter to specify what columns to generate
	 * @return the result of the update
	 * @throws NullPointerException if {@code sql} or {@code bindings} is
	 *                              {@code null}
	 */
	ResultWithGeneratedKeys update(CommandInterface command, Object generatedColumns);

	/**
	 *
	 * Return back the current {@link Session} to the database.
	 *
	 * @return the current {@link Session} to the database.
	 */
	Session getSession();
}
