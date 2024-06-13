package org.vanilladb.core.storage.index.IVF;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;
import jdk.incubator.vector.VectorOperators;

public class SIMDOperations {
    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    public static float simdEuclideanDistance(float[] queryVector, float[] vector2) {
        assert queryVector.length == vector2.length : "Vectors must be same length";
        float result = 0.0f;
        int i = 0;
        int length = queryVector.length;

        for (; i < SPECIES.loopBound(length); i += SPECIES.length()) {
            FloatVector v1 = FloatVector.fromArray(SPECIES, queryVector, i);
            FloatVector v2 = FloatVector.fromArray(SPECIES, vector2, i);

            FloatVector diff = v1.sub(v2);
            FloatVector squared = diff.mul(diff);
            result += squared.reduceLanes(VectorOperators.ADD);
        }

        for (; i < length; i++) {
            float diff = queryVector[i] - vector2[i];
            result += diff * diff;
        }

        return (float) Math.sqrt(result);
    }
}