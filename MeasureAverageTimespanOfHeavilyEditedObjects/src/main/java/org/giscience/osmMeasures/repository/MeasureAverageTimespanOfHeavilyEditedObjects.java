package org.giscience.osmMeasures.repository;

import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.measures.tools.Index;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.SortedMap;

public class MeasureAverageTimespanOfHeavilyEditedObjects extends MeasureOSHDB<Number, OSMContribution> {

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
                .osmType(OSMType.WAY)

                // Filter HEO
                .osmEntityFilter(entity -> entity.getVersion() >= 15)

                // Time difference in days per OSHEntity
                .map(osmEntityContribution -> {
                    Object ListModificationTimestamps = osmEntityContribution.getOSHEntity().getModificationTimestamps();
                    OSHDBTimestamp creationTimestamp = (OSHDBTimestamp) ((List) ListModificationTimestamps).get(0);
                    Integer sizeListModificationTimestamps = ((List) ListModificationTimestamps).size();
                    OSHDBTimestamp lastVersionTimestamp = (OSHDBTimestamp) ((List) ListModificationTimestamps).get(sizeListModificationTimestamps-1);

                    String s1 = creationTimestamp.toString().split("T")[0];
                    String s2 = lastVersionTimestamp.toString().split("T")[0];
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                    Date d1 = null;
                    Date d2= null;
                    try {
                        d1 = df.parse(s1);
                        d2 = df.parse(s2);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    return (d2.getTime() - d1.getTime()) / (60*60*1000*24);
                })
                .collect(),

                x -> {

        // output
            return x.stream().mapToLong(y -> y).average().orElse(0.0);
        // EXAMPLE END
    }));
}
}
