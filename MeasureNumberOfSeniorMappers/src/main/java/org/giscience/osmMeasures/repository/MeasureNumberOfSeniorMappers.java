package org.giscience.osmMeasures.repository;

import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.rest.server.RequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.measures.tools.Index;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;

import java.util.*;

public class MeasureNumberOfSeniorMappers extends MeasureOSHDB<Number, OSMContribution> {

/*
    @Override
    public Boolean refersToTimeSpan() {
        return false;
    }

    @Override
    public Integer defaultDaysBefore() {
        return 3 * 12 * 30;
    }

    @Override
    public Integer defaultIntervalInDays() {
        return 30;
    }
*/

    @Override
    public SortedMap<GridCell, Number> compute(MapAggregator<GridCell, OSMContribution> mapReducer, OSHDBRequestParameter p) throws Exception {
        // EXAMPLE ONLY - PLEASE INSERT CODE HERE
        return Cast.result(Index.map(mapReducer
                        // -- ENTITY FILTER --
                        .osmType(OSMType.NODE)

                        // Filtering by ContributionType CREATION
                        .filter(contribution -> contribution.getContributionTypes().contains(ContributionType.CREATION))

                        // -- MAPPING --
                        // Get only versions of creation with userID only CREATION?
                        .map(OSMContribution::getContributorUserId)

                        // -- AGGREGATION --
                        .collect(),

                x -> {
                    Map<Integer, Integer> result1 = new HashMap<>();
                    for (Integer unique : new HashSet<>(x)) {
                        result1.put(unique, Collections.frequency(x, unique));
                    }


                    // Filter Senior Mappers
                    return result1.entrySet().stream()
                            .filter(t -> t.getValue() >= 1000)
                            .count();

                }));
    }


        // EXAMPLE END

}
