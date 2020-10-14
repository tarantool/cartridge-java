package io.tarantool.driver.metadata;

import io.tarantool.driver.exceptions.TarantoolIndexNotFoundException;

import java.util.Map;
import java.util.Optional;

public class TestSpaceMetadata implements TarantoolSpaceMetadataOperations {

    private final TestMetadata testMetadata;

    public TestSpaceMetadata(TestMetadata testMetadata) {
        this.testMetadata = testMetadata;
    }

    @Override
    public TarantoolIndexMetadata getIndexById(int indexId) {
        Optional<TarantoolIndexMetadata> indexMetadata =
                getSpaceIndexes().values().stream().filter(i -> i.getIndexId() == indexId).findFirst();
        if (!indexMetadata.isPresent()) {
            throw new TarantoolIndexNotFoundException(TestMetadata.SPACE_NAME, indexId);
        }
        return indexMetadata.get();
    }

    @Override
    public TarantoolIndexMetadata getIndexByName(String indexName) {
        if (!getSpaceIndexes().containsKey(indexName)) {
            throw new TarantoolIndexNotFoundException(TestMetadata.SPACE_NAME, indexName);
        }
        return getSpaceIndexes().get(indexName);
    }

    @Override
    public Map<String, TarantoolIndexMetadata> getSpaceIndexes() {
        return testMetadata.getIndexMetadata().get(TestMetadata.SPACE_NAME);
    }

    @Override
    public TarantoolSpaceMetadata getSpaceMetadata() {
        return testMetadata.getTestSpaceMetadata();
    }
}
