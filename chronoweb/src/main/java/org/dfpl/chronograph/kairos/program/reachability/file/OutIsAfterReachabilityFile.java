package org.dfpl.chronograph.kairos.program.reachability.file;

import com.tinkerpop.blueprints.*;
import org.bson.Document;
import org.dfpl.chronograph.common.EdgeEvent;
import org.dfpl.chronograph.common.TemporalRelation;
import org.dfpl.chronograph.common.VertexEvent;
import org.dfpl.chronograph.kairos.AbstractKairosProgram;
import org.dfpl.chronograph.kairos.gamma.GammaTable;
import org.dfpl.chronograph.kairos.gamma.persistent.file.FixedSizedGammaTable;
import org.dfpl.chronograph.kairos.gamma.persistent.file.LongGammaElement;
import org.dfpl.chronograph.kairos.program.reachability.file.algorithms.TimeCentricReachability;
import org.dfpl.chronograph.kairos.program.reachability.file.algorithms.TraversalReachability;
import org.dfpl.chronograph.khronos.manipulation.memory.MChronoGraph;
import org.dfpl.chronograph.khronos.manipulation.memory.MChronoVertex;
import org.dfpl.chronograph.khronos.manipulation.memory.MChronoVertexEvent;
import org.dfpl.chronograph.khronos.manipulation.persistent.PChronoGraph;

import java.io.*;
import java.nio.file.NotDirectoryException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class OutIsAfterReachabilityFile extends AbstractKairosProgram<Long> {
    public static final TemporalRelation TR = TemporalRelation.isAfter;

    /**
     * Return true if the source value has a valid value
     */
    public static final Predicate<Long> IS_SOURCE_VALID = t -> t != 9187201950435737471L;

    /**
     * Return true if the second argument is less than the first argument
     */
    public static final BiPredicate<Long, Long> IS_AFTER = (t, u) -> u < t;
    private final static BiPredicate<Long, Long> IS_COTEMPORAL = (t, u) -> Objects.equals(u, t);

    public OutIsAfterReachabilityFile(Graph graph, GammaTable<String, Long> gammaTable) {
        super(graph, gammaTable, "OutIsAfterReachability");
    }

    public static FixedSizedGammaTable<String, Long> createGammaTable(String subDirectoryName) throws NotDirectoryException, FileNotFoundException {
        File subDirectory = new File(subDirectoryName);
        if (!subDirectory.exists())
            subDirectory.mkdirs();

        return new FixedSizedGammaTable<>(subDirectoryName, LongGammaElement.class);
    }

    @Override
    public void onInitialization(Set<Vertex> sources, Long startTime, String edgeLabel) {
        this.edgeLabel = edgeLabel;

        synchronized (this.gammaTable) {
            for (Vertex sourceVertex : sources) {
                this.gammaTable.addSource(sourceVertex.getId(), new LongGammaElement(startTime));
            }

            if (graph instanceof MChronoGraph) {
                new TimeCentricReachability(this.graph, this.gammaTable).compute(sources, startTime, TR, this.edgeLabel, true);
            } else if (graph instanceof PChronoGraph pg) {
                pg.getEdgeEvents().forEach(event -> {
                    this.gammaTable.update(sources.parallelStream().map(Element::getId).collect(Collectors.toSet()),
                            event.getVertex(Direction.OUT).getId(), IS_SOURCE_VALID, event.getVertex(Direction.IN).getId(),
                            new LongGammaElement(event.getTime()), IS_AFTER);
                });
            }
        }
    }

    @Override
    public void onAddEdgeEvent(EdgeEvent addedEvent) {
        File resultFile = new File("D:\\tpvis\\results\\CollegeMsgFile.txt");
        try {
            FileWriter resultFW = new FileWriter(resultFile, true);
            BufferedWriter resultBW = new BufferedWriter(resultFW);

            long pre = System.currentTimeMillis();

            synchronized (this.gammaTable) {
                Vertex iPrime = addedEvent.getVertex(Direction.OUT);
                Vertex jPrime = addedEvent.getVertex(Direction.IN);

                // Step 1: Computing affected subgraph
                VertexEvent sourcePrime = new MChronoVertexEvent(jPrime, addedEvent.getTime());
                String gammaPrimePath = String.format("%s\\onAdd\\%s", ((FixedSizedGammaTable<String, Long>) this.gammaTable).getDirectory().getPath(), sourcePrime.getId());

                try {
                    TraversalReachability algorithm = new TraversalReachability(gammaPrimePath);
                    Map<String, LongGammaElement> gammaPrime = new HashMap<>();
                    algorithm.compute(this.graph, sourcePrime, TR, this.edgeLabel)
                            .toMap(true).entrySet().stream().filter(entry -> entry.getValue() != null)
                            .forEach(entry -> {
                                gammaPrime.put(entry.getKey(), new LongGammaElement(entry.getValue()));
                            });

                    // Step 2: Updating the Gamma Table
                    ((FixedSizedGammaTable) this.gammaTable).update(iPrime.getId(), IS_SOURCE_VALID, addedEvent.getTime(), gammaPrime, IS_AFTER);

                    algorithm.getGammaTable().clear();

                } catch (NotDirectoryException | FileNotFoundException e) {
                    e.printStackTrace();
                }
            }

            long computationTime = System.currentTimeMillis() - pre;
            resultBW.write(computationTime + "\n");

            resultBW.close();
            resultFW.close();

//            this.gammaTable.print();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onRemoveEdgeEvent(EdgeEvent removedEdge) {
        // Step 1: Compute immediate results using traversal approach
        Vertex iPrime = removedEdge.getVertex(Direction.OUT);

        VertexEvent sourcePrime = new MChronoVertexEvent(iPrime, removedEdge.getTime());
        String gammaPrimePath = String.format("%s\\onRemove\\%s", ((FixedSizedGammaTable<String, Long>) this.gammaTable).getDirectory().getPath(), sourcePrime.getId());

        try {
            TraversalReachability algorithm = new TraversalReachability(gammaPrimePath);
            Set<String> gammaPrime = algorithm.computeInverse(this.graph, sourcePrime, TemporalRelation.isBefore, this.edgeLabel)
                    .toMap(true).entrySet().stream().filter(entry -> entry.getValue() != null)
                    .map(Map.Entry::getKey).collect(Collectors.toSet());

            // Step 2: Invalidate gamma values
            Set<Vertex> sourcesPrime = this.gammaTable.getSources().stream().filter(gammaPrime::contains).map(id -> new MChronoVertex(this.graph, id)).collect(Collectors.toSet());

            for (Vertex source : sourcesPrime) {
                Map<String, Long> gamma = this.gammaTable.getGamma(source.getId()).toMap(true);

                for (Map.Entry<String, Long> entry : gamma.entrySet()) {
                    Long currValue = gamma.get(entry.getKey());

                    if (currValue == null)
                        continue;

                    if (IS_AFTER.test(currValue, removedEdge.getTime()) || IS_COTEMPORAL.test(currValue, removedEdge.getTime())) {
                        this.gammaTable.invalidate(source.getId(), entry.getKey());
                    }
                }
            }
            algorithm.getGammaTable().clear();

            // Step 3: Re-compute Gamma Table using time-centric approach
            new TimeCentricReachability(this.graph, this.gammaTable).compute(sourcesPrime, removedEdge.getTime(), TR, this.edgeLabel, true);

        } catch (NotDirectoryException | FileNotFoundException e) {
            e.printStackTrace();
        }

        this.gammaTable.print();
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
