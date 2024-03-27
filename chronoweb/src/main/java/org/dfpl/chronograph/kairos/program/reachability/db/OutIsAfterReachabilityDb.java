package org.dfpl.chronograph.kairos.program.reachability.db;

import com.tinkerpop.blueprints.*;
import org.bson.Document;
import org.dfpl.chronograph.common.EdgeEvent;
import org.dfpl.chronograph.common.VertexEvent;
import org.dfpl.chronograph.kairos.AbstractKairosProgram;
import org.dfpl.chronograph.kairos.gamma.GammaTable;
import org.dfpl.chronograph.kairos.gamma.persistent.db.LongGammaElement;
import org.dfpl.chronograph.khronos.manipulation.persistent.PChronoGraph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class OutIsAfterReachabilityDb extends AbstractKairosProgram<Document> {

    /**
     * Return true if the source value has a valid value
     */
    public static final Predicate<Document> IS_SOURCE_VALID = Objects::nonNull;

    /**
     * Return true if the second argument is less than the first argument
     */
    public static final BiPredicate<Document, Document> IS_AFTER = (t, u) -> u.getLong("time") < t.getLong("time");

    public OutIsAfterReachabilityDb(Graph graph, GammaTable<String, Document> gammaTable) {
        super(graph, gammaTable, "OutIsAfterReachability");
    }

    @Override
    public void onInitialization(Set<Vertex> sources, Long startTime, String edgeLabel) {
        this.edgeLabel = edgeLabel;

        synchronized (this.gammaTable) {
            for (Vertex sourceVertex : sources) {
                this.gammaTable.addSource(sourceVertex.getId(), new LongGammaElement(startTime));
            }
            ((PChronoGraph) this.graph).getEdgeEvents().forEach(event -> {
                this.gammaTable.update(sources.parallelStream().map(Element::getId).collect(Collectors.toSet()),
                        event.getVertex(Direction.OUT).getId(), IS_SOURCE_VALID, event.getVertex(Direction.IN).getId(),
                        new LongGammaElement(event.getTime()), IS_AFTER);
            });
        }

        this.gammaTable.print();
    }

    @Override
    public void onAddEdgeEvent(EdgeEvent addedEvent) {
        File resultFile = new File("D:\\tpvis\\results\\CollegeMsgGamma.txt");
        try {
            FileWriter resultFW = new FileWriter(resultFile, true);
            BufferedWriter resultBW = new BufferedWriter(resultFW);

            long pre = System.currentTimeMillis();

            synchronized (this.gammaTable) {
                this.gammaTable.update(addedEvent.getVertex(Direction.OUT).getId(), IS_SOURCE_VALID,
                        addedEvent.getVertex(Direction.IN).getId(),
                        new LongGammaElement(addedEvent.getTime()), IS_AFTER
                );
            }

            long computationTime = System.currentTimeMillis() - pre;
            resultBW.write(computationTime + "\n");

            resultBW.close();
            resultFW.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.gammaTable.print();
    }

    @Override
    public void onRemoveEdgeEvent(EdgeEvent removedEdge) {

    }

    @Override
    public void onAddVertex(Vertex addedVertex) {

    }

    @Override
    public void onAddEdge(Edge addedEdge) {

    }

    @Override
    public void onUpdateVertexProperty(Document previous, Document updated) {

    }

    @Override
    public void onUpdateEdgeProperty(Document previous, Document updated) {

    }

    @Override
    public void onRemoveVertex(Vertex removedVertex) {

    }

    @Override
    public void onRemoveEdge(Edge removedEdge) {

    }

    @Override
    public void onAddVertexEvent(VertexEvent addedVertexEvent) {

    }

    @Override
    public void onUpdateVertexEventProperty(Document previous, Document updated) {

    }

    @Override
    public void onUpdateEdgeEventProperty(Document previous, Document updated) {

    }

    @Override
    public void onRemoveVertexEvent(VertexEvent removedVertex) {

    }
}
