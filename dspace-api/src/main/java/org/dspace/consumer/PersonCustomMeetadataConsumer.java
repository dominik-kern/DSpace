/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.consumer;

import static java.util.Arrays.asList;
import static org.apache.logging.log4j.util.Strings.isNotBlank;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;
import org.dspace.content.Item;
import org.dspace.content.MetadataFieldName;
import org.dspace.content.factory.ContentServiceFactory;
import org.dspace.content.service.ItemService;
import org.dspace.core.Context;
import org.dspace.event.Consumer;
import org.dspace.event.Event;
import org.dspace.services.ConfigurationService;
import org.dspace.services.factory.DSpaceServicesFactory;

/**
 * Implementation of {@link Consumer} that adds custom metadata to Person.
 */
public class PersonCustomMeetadataConsumer implements Consumer {

    private final static Logger log = org.apache.logging.log4j.LogManager
            .getLogger(PersonCustomMeetadataConsumer.class);

    private static final String METADATA_PLACEHOLDER = "\\{([^{}]+)\\}";

    private final Set<Item> itemsAlreadyProcessed = new HashSet<>();

    private List<String> metadataNames;

    private List<String> metadataFormat;

    private ConfigurationService configurationService;

    private ItemService itemService;

    @Override
    public void initialize() throws Exception {
        configurationService = DSpaceServicesFactory.getInstance().getConfigurationService();
        itemService = ContentServiceFactory.getInstance().getItemService();

        metadataNames = asList(configurationService.getArrayProperty("event.consumer.person-metadata.metadata"));
        metadataFormat = asList(configurationService.getArrayProperty("event.consumer.person-metadata.format"));
    }

    @Override
    public void consume(Context context, Event event) throws Exception {
        Item item = (Item) event.getSubject(context);
        if (item == null || !item.isArchived() || itemsAlreadyProcessed.contains(item) || !isPerson(item)) {
            return;
        }

        itemsAlreadyProcessed.add(item);

        context.turnOffAuthorisationSystem();
        try {
            addCustomMetadata(context, item);
        } catch (Exception exception) {
            log.error(exception.getMessage(), exception);
        } finally {
            context.restoreAuthSystemState();
        }
    }

    @Override
    public void end(Context ctx) throws Exception {
        itemsAlreadyProcessed.clear();
    }

    @Override
    public void finish(Context ctx) throws Exception {

    }

    private void addCustomMetadata(Context context, Item person) throws SQLException {
        for (int i = 0; i < metadataNames.size(); i++) {
            addCustomMetadata(context, person, metadataNames.get(i), metadataFormat.get(i));
        }
    }

    private void addCustomMetadata(Context context, Item person, String metadataName, String metadataFormat)
            throws SQLException {
        String metadataValue = generateMetadataValue(person, metadataFormat);
        MetadataFieldName field = new MetadataFieldName(metadataName);

        itemService.clearMetadata(context, person, field.schema, field.element, field.qualifier, Item.ANY);
        if (isNotBlank(metadataValue)) {
            itemService.addMetadata(
                    context, person, field.schema, field.element, field.qualifier, null, metadataValue);
        }
    }

    private String generateMetadataValue(Item person, String metadataFormat) {
        for (String placeholderMetadata : getPlaceholderMetadataNames(metadataFormat)) {
            MetadataFieldName field = new MetadataFieldName(placeholderMetadata);
            String placeholderValue = itemService.getMetadataFirstValue(person, field, Item.ANY);
            metadataFormat = metadataFormat.replace("{" + placeholderMetadata + "}", placeholderValue);
        }
        return metadataFormat;
    }

    private boolean isPerson(Item item) {
        String type = itemService.getMetadataFirstValue(item, "dspace", "entity", "type", Item.ANY);
        return "Person".equalsIgnoreCase(type);
    }

    private static List<String> getPlaceholderMetadataNames(String metadataFormat) {
        Pattern pattern = Pattern.compile(METADATA_PLACEHOLDER);
        Matcher matcher = pattern.matcher(metadataFormat);

        List<String> placeholderMetadata = new ArrayList<>();
        while (matcher.find()) {
            placeholderMetadata.add(matcher.group(1));
        }
        return placeholderMetadata;
    }
}