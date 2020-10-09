package io.tarantool.driver.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author Alexey Kuzin
 */
public class TestMetadata extends AbstractTarantoolMetadata {

    private final TarantoolSpaceMetadata testSpaceMetadata;
    private final TarantoolIndexMetadata testPrimaryIndexMetadata;
    private final TarantoolIndexMetadata testIndexMetadata1;
    private final TarantoolIndexMetadata testIndexMetadata2;
    private final TarantoolIndexMetadata testIndexMetadata3;
    private final TarantoolIndexMetadata testIndexMetadata4;

    public TestMetadata() {
        testSpaceMetadata = new TarantoolSpaceMetadata();
        testSpaceMetadata.setSpaceId(512);
        testSpaceMetadata.setSpaceName("test");
        TarantoolFieldMetadata firstFieldMetadata = new TarantoolFieldMetadata("first", "string", 0);
        TarantoolFieldMetadata secondFieldMetadata = new TarantoolFieldMetadata("second", "number", 1);
        TarantoolFieldMetadata thirdFieldMetadata = new TarantoolFieldMetadata("third", "number", 2);
        TarantoolFieldMetadata fourthFieldMetadata = new TarantoolFieldMetadata("fourth", "number", 3);
        Map<String, TarantoolFieldMetadata> fieldMetadataMap = new HashMap<>();
        fieldMetadataMap.put("first", firstFieldMetadata);
        fieldMetadataMap.put("second", secondFieldMetadata);
        fieldMetadataMap.put("third", thirdFieldMetadata);
        fieldMetadataMap.put("fourth", fourthFieldMetadata);
        testSpaceMetadata.setSpaceFormatMetadata(fieldMetadataMap);

        testPrimaryIndexMetadata = new TarantoolIndexMetadata();
        testPrimaryIndexMetadata.setIndexId(0);
        testPrimaryIndexMetadata.setIndexName("primary");
        testPrimaryIndexMetadata.setSpaceId(512);
        testPrimaryIndexMetadata.setIndexParts(Collections.singletonList(new TarantoolIndexPartMetadata(0, "string")));

        testIndexMetadata2 = new TarantoolIndexMetadata();
        testIndexMetadata2.setIndexId(1);
        testIndexMetadata2.setIndexName("asecondary1");
        testIndexMetadata2.setSpaceId(512);
        List<TarantoolIndexPartMetadata> parts = new ArrayList<>();
        parts.add(new TarantoolIndexPartMetadata(1, "number"));
        parts.add(new TarantoolIndexPartMetadata(2, "number"));
        testIndexMetadata2.setIndexParts(parts);

        testIndexMetadata3 = new TarantoolIndexMetadata();
        testIndexMetadata3.setIndexId(2);
        testIndexMetadata3.setIndexName("secondary2");
        testIndexMetadata3.setSpaceId(512);
        parts = new ArrayList<>();
        parts.add(new TarantoolIndexPartMetadata(1, "number"));
        parts.add(new TarantoolIndexPartMetadata(3, "number"));
        testIndexMetadata3.setIndexParts(parts);

        testIndexMetadata4 = new TarantoolIndexMetadata();
        testIndexMetadata4.setIndexId(3);
        testIndexMetadata4.setIndexName("asecondary3");
        testIndexMetadata4.setSpaceId(512);
        parts = new ArrayList<>();
        parts.add(new TarantoolIndexPartMetadata(1, "number"));
        parts.add(new TarantoolIndexPartMetadata(2, "number"));
        parts.add(new TarantoolIndexPartMetadata(3, "number"));
        testIndexMetadata4.setIndexParts(parts);

        testIndexMetadata1 = new TarantoolIndexMetadata();
        testIndexMetadata1.setIndexId(4);
        testIndexMetadata1.setIndexName("asecondary");
        testIndexMetadata1.setSpaceId(512);
        testIndexMetadata1.setIndexParts(Collections.singletonList(new TarantoolIndexPartMetadata(1, "number")));
    }

    public TarantoolSpaceMetadata getTestSpaceMetadata() {
        return testSpaceMetadata;
    }

    public TarantoolIndexMetadata getTestPrimaryIndexMetadata() {
        return testPrimaryIndexMetadata;
    }

    public TarantoolIndexMetadata getTestIndexMetadata1() {
        return testIndexMetadata1;
    }
    public TarantoolIndexMetadata getTestIndexMetadata2() {
        return testIndexMetadata2;
    }

    public TarantoolIndexMetadata getTestIndexMetadata3() {
        return testIndexMetadata3;
    }

    public TarantoolIndexMetadata getTestIndexMetadata4() {
        return testIndexMetadata4;
    }

    @Override
    protected CompletableFuture<Void> populateMetadata() {

        spaceMetadata.put("test", testSpaceMetadata);
        spaceMetadataById.put(512, testSpaceMetadata);

        Map<String, TarantoolIndexMetadata> indexes = new HashMap<>();
        indexes.put("primary", testPrimaryIndexMetadata);
        indexes.put("asecondary", testIndexMetadata1);
        indexes.put("asecondary1", testIndexMetadata2);
        indexes.put("secondary2", testIndexMetadata3);
        indexes.put("asecondary3", testIndexMetadata4);

        indexMetadata.put("test", indexes);
        indexMetadataBySpaceId.put(512, indexes);

        return CompletableFuture.completedFuture(null);
    }
}
