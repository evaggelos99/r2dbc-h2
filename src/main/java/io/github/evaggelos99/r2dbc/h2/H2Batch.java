package io.github.evaggelos99.r2dbc.h2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;

import io.github.evaggelos99.r2dbc.h2.client.Client;
import io.github.evaggelos99.r2dbc.h2.codecs.Codecs;
import io.github.evaggelos99.r2dbc.h2.util.Assert;
import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

/**
 * An implementation of {@link Batch} for executing a collection of statements
 * in a batch against an H2 database.
 */
public final class H2Batch implements Batch {

	private final Client client;

	private final Codecs codecs;

	private final List<String> statements = new ArrayList<>();

	H2Batch(Client client, Codecs codecs) {
		this.client = Assert.requireNonNull(client, "client must not be null");
		this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
	}

	@Override
	public H2Batch add(String sql) {
		Assert.requireNonNull(sql, "sql must not be null");

		this.statements.add(sql);
		return this;
	}

	@Override
	public Flux<H2Result> execute() {
		return Flux.fromIterable(this.statements)
				.flatMapIterable(statement -> () -> this.client.prepareCommand(statement, Collections.emptyList()))
				.map(command -> {
					try {
						if (command.isQuery()) {

							ResultInterface result = this.client.query(command);
							CommandUtil.clearForReuse(command);
							return H2Result.toResult(this.codecs, result, null);
						} else {

							ResultWithGeneratedKeys result = this.client.update(command, false);
							CommandUtil.clearForReuse(command);
							long updatedCountInt = result.getUpdateCount();
							return H2Result.toResult(this.codecs, updatedCountInt);
						}
					} catch (DbException e) {
						throw H2DatabaseExceptionFactory.convert(e);
					}
				});
	}

}
