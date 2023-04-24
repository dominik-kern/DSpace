/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.app.bulkimport.converter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.dspace.authorize.ResourcePolicy;
import org.dspace.authorize.service.ResourcePolicyService;
import org.dspace.content.Bitstream;
import org.dspace.content.Bundle;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.content.dto.BitstreamDTO;
import org.dspace.content.dto.ItemDTO;
import org.dspace.content.dto.MetadataValueDTO;
import org.dspace.content.dto.ResourcePolicyDTO;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.services.ConfigurationService;
import org.springframework.beans.factory.annotation.Autowired;

public class ItemToItemDTOConverter implements ItemDTOConverter<Item> {

    private static final String BITSTREAM_URL_FORMAT = "%s/api/core/bitstreams/%s/content";

    @Autowired
    private ConfigurationService configurationService;

    @Autowired
    private ResourcePolicyService resourcePolicyService;

    @Override
    public ItemDTO convert(Context context, Item item) {
        String id = item.getID().toString();
        List<MetadataValueDTO> metadataValues = getMetadataValues(item);
        List<BitstreamDTO> bitstreams = getBitstreams(context, item);
        return new ItemDTO(id, item.isDiscoverable(), metadataValues, bitstreams);
    }

    private List<MetadataValueDTO> getMetadataValues(DSpaceObject dso) {
        return dso.getMetadata().stream()
            .map(MetadataValueDTO::new)
            .collect(Collectors.toList());
    }

    private List<BitstreamDTO> getBitstreams(Context context, Item item) {

        List<BitstreamDTO> bitstreams = new ArrayList<>();

        for (Bundle bundle : item.getBundles()) {

            String bundleName = bundle.getName();
            int position = 1;

            for (Bitstream bitstream : bundle.getBitstreams()) {
                bitstreams.add(buildBitstreamDTO(context, bitstream, bundleName, position++));
            }

        }

        return bitstreams;
    }

    private BitstreamDTO buildBitstreamDTO(Context context, Bitstream bitstream, String bundleName, int position) {
        String location = getBitstreamLocationUrl(bitstream);
        List<MetadataValueDTO> metadataValues = getMetadataValues(bitstream);
        List<ResourcePolicyDTO> policies = getCustomReadResourcePolicies(context, bitstream);
        return new BitstreamDTO(bundleName, position, location, metadataValues, policies);
    }

    private String getBitstreamLocationUrl(Bitstream bitstream) {
        String dspaceServerUrl = configurationService.getProperty("dspace.server.url");
        return String.format(BITSTREAM_URL_FORMAT, dspaceServerUrl, bitstream.getID().toString());
    }

    private List<ResourcePolicyDTO> getCustomReadResourcePolicies(Context context, Bitstream bitstream) {
        try {
            return resourcePolicyService.find(context, bitstream, Constants.READ, ResourcePolicy.TYPE_CUSTOM).stream()
                .map(ResourcePolicyDTO::new)
                .collect(Collectors.toList());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
