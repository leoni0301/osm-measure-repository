package org.giscience.osmMeasures.repository;

import org.giscience.measures.rest.measure.MeasureOSHDB;
import org.giscience.measures.rest.server.RequestParameter;
import org.giscience.measures.rest.server.OSHDBRequestParameter;
import org.giscience.measures.tools.Cast;
import org.giscience.utils.geogrid.cells.GridCell;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

import java.util.SortedMap;

public class MeasureNumberOfHeavilyEditedObjects extends MeasureOSHDB<Number, OSMEntitySnapshot> {

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
    public SortedMap<GridCell, Number> compute(MapAggregator<GridCell, OSMEntitySnapshot> mapReducer, RequestParameter p) throws Exception {
        // EXAMPLE ONLY - PLEASE INSERT CODE HERE
        return Cast.result(mapReducer
                .osmType(OSMType.WAY)
                .osmTag("highway", "motorway")
                .osmEntityFilter( entity -> entity.getVersion() >= 15)
                .map(OSMEntitySnapshot::getEntity)
                .count());

        // EXAMPLE END
    }
}
