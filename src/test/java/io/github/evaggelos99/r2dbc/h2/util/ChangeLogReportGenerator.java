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

package io.github.evaggelos99.r2dbc.h2.util;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

import org.springframework.hateoas.IanaLinkRelations;
import org.springframework.hateoas.Links;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClient;

import com.jayway.jsonpath.JsonPath;

import net.minidev.json.JSONArray;

/**
 * Changelog report generator.
 */
final class ChangeLogReportGenerator {

	private static final int MILESTONE_ID = 18;
	private static final String URI_TEMPLATE = "https://api.github.com/repos/r2dbc/r2dbc-h2/issues?milestone={id}&state=closed";

	public static void main(final String... args) {

		/*
		 * If you run into github rate limiting issues, you can always use a Github
		 * Personal Token by adding {@code .header(HttpHeaders.AUTHORIZATION,
		 * "token your-github-token")} to the webClient call.
		 */

		final WebClient webClient = WebClient.create();

		HttpEntity<String> response = webClient //
				.get().uri(URI_TEMPLATE, MILESTONE_ID) //
				.exchange() //
				.flatMap(clientResponse -> clientResponse.toEntity(String.class)) //
				.block(Duration.ofSeconds(10));

		boolean keepChecking = true;
		boolean printHeader = true;

		while (keepChecking) {

			readPage(response.getBody(), printHeader);
			printHeader = false;

			final List<String> linksInHeader = response.getHeaders().get(HttpHeaders.LINK);
			final Links links = linksInHeader == null ? Links.NONE : Links.parse(linksInHeader.get(0));

			if (links.getLink(IanaLinkRelations.NEXT).isPresent()) {

				response = webClient //
						.get().uri(links.getRequiredLink(IanaLinkRelations.NEXT).expand().getHref()) //
						.exchange() //
						.flatMap(clientResponse -> clientResponse.toEntity(String.class)) //
						.block(Duration.ofSeconds(10));

			} else {
				keepChecking = false;
			}
		}
	}

	private static void readPage(final String content, final boolean header) {

		final JsonPath titlePath = JsonPath.compile("$[*].title");
		final JsonPath idPath = JsonPath.compile("$[*].number");

		final JSONArray titles = titlePath.read(content);
		final Iterator<Object> ids = ((JSONArray) idPath.read(content)).iterator();

		if (header) {
			System.out.println(JsonPath.read(content, "$[1].milestone.title").toString());
			System.out.println("------------------");
		}

		for (final Object title : titles) {

			final String format = String.format("* %s #%s", title, ids.next());
			System.out.println(format.endsWith(".") ? format : format.concat("."));
		}
	}
}
