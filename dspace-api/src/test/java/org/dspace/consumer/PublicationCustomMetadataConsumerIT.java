/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.consumer;

import static org.dspace.app.matcher.MetadataValueMatcher.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.dspace.AbstractIntegrationTestWithDatabase;
import org.dspace.builder.CollectionBuilder;
import org.dspace.builder.CommunityBuilder;
import org.dspace.builder.ItemBuilder;
import org.dspace.content.Collection;
import org.dspace.content.Item;
import org.dspace.event.ConsumerProfile;
import org.dspace.event.Dispatcher;
import org.dspace.event.factory.EventServiceFactory;
import org.dspace.event.service.EventService;
import org.dspace.services.ConfigurationService;
import org.dspace.services.factory.DSpaceServicesFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for {@link PersonCustomMetadataConsumer}.
 */
public class PersonCustomMetadataConsumerIT extends AbstractIntegrationTestWithDatabase {

    private final ConfigurationService configurationService = DSpaceServicesFactory
            .getInstance().getConfigurationService();

    private final EventService eventService = EventServiceFactory.getInstance().getEventService();

    private Collection collection;

    @Before
    public void setup() {
        context.turnOffAuthorisationSystem();

        parentCommunity = CommunityBuilder.createCommunity(context)
                .withName("Parent Community")
                .build();

        collection = CollectionBuilder.createCollection(context, parentCommunity)
                .withName("Collection 1")
                .withEntityType("Person")
                .build();

        configurationService.setProperty("cris.custom-url.consumer.supported-entities", "Person");

        context.restoreAuthSystemState();
    }

    @Test
    public void testAddCustomMetadata() throws SQLException {

        context.turnOffAuthorisationSystem();

        configurationService.setProperty("event.consumer.publication-metadata.metadata", "local.nameHeader");
        configurationService.setProperty(
                "event.consumer.publication-metadata.format", "{publication.givenName}\\, {publication.familyName}");

        callConsumerInitMethod();

        Item item = ItemBuilder.createItem(context, collection)
                .withTitle("TestPerson")
                .withMetadata("publication", "givenName", null, "GivenName")
                .withMetadata("publication", "familyName", null, "FamilyName")
                .build();

        context.restoreAuthSystemState();
        context.commit();

        item = context.reloadEntity(item);

        assertThat(item.getMetadata(), hasItem(with("local.nameHeader", "GivenName, FamilyName")));
    }

    @Test
    public void testAddMultipleCustomMetadata() throws SQLException {

        context.turnOffAuthorisationSystem();

        configurationService.setProperty("event.consumer.publication-metadata.metadata", "dc.title");
        configurationService.setProperty("event.consumer.publication-metadata.format", "Test {publication.givenName}");

        configurationService.addPropertyValue("event.consumer.publication-metadata.metadata", "local.nameHeader");
        configurationService.addPropertyValue(
                "event.consumer.publication-metadata.format", "{publication.givenName}\\, {publication.familyName}");

        callConsumerInitMethod();

        Item item = ItemBuilder.createItem(context, collection)
                .withTitle("TestPerson")
                .withMetadata("publication", "givenName", null, "GivenName")
                .withMetadata("publication", "familyName", null, "FamilyName")
                .build();

        context.restoreAuthSystemState();
        context.commit();

        item = context.reloadEntity(item);

        assertThat(item.getMetadata(), hasItem(with("local.nameHeader", "GivenName, FamilyName")));
        assertThat(item.getMetadata(), hasItem(with("dc.title", "Test GivenName")));
    }

    @Test
    public void testRemoveExistingCustomMetadata() throws SQLException {

        context.turnOffAuthorisationSystem();

        configurationService.setProperty("event.consumer.publication-metadata.metadata", "dc.title");
        configurationService.setProperty("event.consumer.publication-metadata.format", "Test {publication.givenName}");

        callConsumerInitMethod();

        String originalItemTitle = "TestPerson";
        Item item = ItemBuilder.createItem(context, collection)
                .withTitle(originalItemTitle)
                .withMetadata("publication", "givenName", null, "GivenName")
                .withMetadata("publication", "familyName", null, "FamilyName")
                .build();

        context.restoreAuthSystemState();
        context.commit();

        item = context.reloadEntity(item);

        assertThat(item.getMetadata(), hasItem(with("dc.title", "Test GivenName")));
        assertThat(item.getMetadata(), not(hasItem(with("dc.title", originalItemTitle))));
    }

    private void callConsumerInitMethod() {
        Dispatcher dispatcher = eventService.getDispatcher("default");
        Object object = dispatcher.getConsumers();
        if (object instanceof Map) {
            Map<String, ConsumerProfile> consumers = (LinkedHashMap<String, ConsumerProfile>) dispatcher.getConsumers();

            ConsumerProfile consumerProfile = consumers.get("publicationcustommetadata");
            try {
                consumerProfile.getConsumer().initialize();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
