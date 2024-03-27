package org.dfpl.chronograph.kairos.program.path_reachability.db;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.bson.Document;
import org.dfpl.chronograph.common.EdgeEvent;
import org.dfpl.chronograph.common.VertexEvent;
import org.dfpl.chronograph.kairos.AbstractKairosProgram;
import org.dfpl.chronograph.kairos.gamma.GammaTable;
import org.dfpl.chronograph.kairos.gamma.persistent.db.PathGammaElement;
import org.dfpl.chronograph.khronos.manipulation.memory.MChronoGraph;
import org.dfpl.chronograph.khronos.manipulation.persistent.PChronoGraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

public class OutIsAfterPathReachabilityDb extends AbstractKairosProgram<Document> {

    public OutIsAfterPathReachabilityDb(Graph graph, GammaTable<String, Document> gammaTable) {
        super(graph, gammaTable, "OutIsAfterPathReachability");
    }

    private static Predicate<Document> IS_SOURCE_VALID = Objects::nonNull;

    private static BiPredicate<Document, Document> IS_AFTER = (t, u) -> u.getLong("time") < t.getLong("time");

    @Override
    public void onInitialization(Set<Vertex> sources, Long startTime, String edgeLabel) {
        this.edgeLabel = edgeLabel;
        synchronized (gammaTable) {
            for (Vertex s : sources) {
                String id = s.getId();
                gammaTable.set(id, id, new PathGammaElement(List.of(id), startTime));
            }

            ((PChronoGraph) graph).getEdgeEvents().forEach(event -> {
                System.out.println("\t\t" + event);
                String out = event.getVertex(Direction.OUT).getId();
                String in = event.getVertex(Direction.IN).getId();
                gammaTable.append(sources.parallelStream().map(v -> v.getId()).collect(Collectors.toSet()), out,
                        IS_SOURCE_VALID, in, new PathGammaElement(List.of(in), event.getTime()), IS_AFTER);
                gammaTable.print();
            });
        }
    }

    @Override
    public void onAddEdgeEvent(EdgeEvent addedEvent) {
        File resultFile = new File("C:\\Users\\haifa\\Desktop\\results\\CollegeMsgDb.txt");

        try {
            FileWriter resultFW = new FileWriter(resultFile, true);
            BufferedWriter resultBW = new BufferedWriter(resultFW);

            long pre = System.currentTimeMillis();

            synchronized (gammaTable) {
                gammaTable.getSources().forEach(source ->{
                    String out = addedEvent.getVertex(Direction.OUT).getId();
                    String in = addedEvent.getVertex(Direction.IN).getId();
                    List<String> newPath;
                    Document temp = this.gammaTable.getGamma(source).getElement(out);
                    if (temp == null)
                        newPath = List.of(in);
                    else{
                        newPath = temp.getList("path", String.class);
                        newPath.add(in);
                    }

                    gammaTable.update(out, IS_SOURCE_VALID, in, new PathGammaElement(newPath, addedEvent.getTime()), IS_AFTER);
                });

                gammaTable.print();
            }

            long computationTime = System.currentTimeMillis() - pre;
            resultBW.write(computationTime + "\n");

            resultBW.close();
            resultFW.close();

        } catch (IOException e){
            e.printStackTrace();
        }

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
