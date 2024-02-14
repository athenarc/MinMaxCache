package gr.imsi.athenarc.visual.middleware.experiments.util;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.List;

public enum Direction {
    L, R;

    public static Direction[] getRandomDirections(int count) {
        List<Pair<Direction, Double>> directionPairs = new ArrayList<>();


        //SYNTH
        directionPairs.add(new Pair<>(L, 0.7d));
        directionPairs.add(new Pair<>(R, 0.3d));

        EnumeratedDistribution<Direction> distribution = new EnumeratedDistribution<>(directionPairs);
        distribution.reseedRandomGenerator(0);
        Direction[] directions = distribution.sample(count, new Direction[count]);
        return directions;
    }
}
