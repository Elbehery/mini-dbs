package de.tuberlin.dima.minidb.api;

import java.io.IOException;
import java.util.logging.Logger;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.Catalogue;
import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.cache.G10PageCache;
import de.tuberlin.dima.minidb.io.cache.PageCache;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.G10BTreeIndex;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.manager.G10BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.G10TablePage;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.mapred.G10TableInputFormat;
import de.tuberlin.dima.minidb.mapred.TableInputFormat;
import de.tuberlin.dima.minidb.mapred.qexec.BulkProcessingOperator;
import de.tuberlin.dima.minidb.mapred.qexec.G10HadoopGroupByOperator;
import de.tuberlin.dima.minidb.mapred.qexec.G10HadoopTableScanOperator;
import de.tuberlin.dima.minidb.mapred.qexec.HadoopOperator;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.optimizer.cost.CostEstimator;
import de.tuberlin.dima.minidb.optimizer.cost.G10CostEstimator;
import de.tuberlin.dima.minidb.optimizer.generator.G10PhysicalPlanGenerator;
import de.tuberlin.dima.minidb.optimizer.generator.PhysicalPlanGenerator;
import de.tuberlin.dima.minidb.optimizer.joins.G10JoinOrderOptimizer;
import de.tuberlin.dima.minidb.optimizer.joins.JoinOrderOptimizer;
import de.tuberlin.dima.minidb.parser.OutputColumn.AggregationType;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.parser.SQLParser;
import de.tuberlin.dima.minidb.qexec.DeleteOperator;
import de.tuberlin.dima.minidb.qexec.FetchOperator;
import de.tuberlin.dima.minidb.qexec.FilterCorrelatedOperator;
import de.tuberlin.dima.minidb.qexec.FilterOperator;
import de.tuberlin.dima.minidb.qexec.G10DeleteOperator;
import de.tuberlin.dima.minidb.qexec.G10FetchOperator;
import de.tuberlin.dima.minidb.qexec.G10FilterCorrelatedOperator;
import de.tuberlin.dima.minidb.qexec.G10FilterOperator;
import de.tuberlin.dima.minidb.qexec.G10GroupByOperator;
import de.tuberlin.dima.minidb.qexec.G10IndexCorrelatedScanOperator;
import de.tuberlin.dima.minidb.qexec.G10IndexLookupOperator;
import de.tuberlin.dima.minidb.qexec.G10IndexScanOperator;
import de.tuberlin.dima.minidb.qexec.G10InsertOperator;
import de.tuberlin.dima.minidb.qexec.G10MergeJoinOperator;
import de.tuberlin.dima.minidb.qexec.G10NestedLoopJoinOperator;
import de.tuberlin.dima.minidb.qexec.G10SortOperator;
import de.tuberlin.dima.minidb.qexec.G10TableScanOperator;
import de.tuberlin.dima.minidb.qexec.GroupByOperator;
import de.tuberlin.dima.minidb.qexec.IndexCorrelatedLookupOperator;
import de.tuberlin.dima.minidb.qexec.IndexLookupOperator;
import de.tuberlin.dima.minidb.qexec.IndexScanOperator;
import de.tuberlin.dima.minidb.qexec.InsertOperator;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.MergeJoinOperator;
import de.tuberlin.dima.minidb.qexec.NestedLoopJoinOperator;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;
import de.tuberlin.dima.minidb.qexec.SortOperator;
import de.tuberlin.dima.minidb.qexec.TableScanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.semantics.SelectQueryAnalyzer;

public class ExtensionFactory extends AbstractExtensionFactory {

	@Override
	public SelectQueryAnalyzer createSelectQueryAnalyzer() {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public TablePage createTablePage(TableSchema schema, byte[] binaryPage) throws PageFormatException {
		
		return new G10TablePage(schema, binaryPage);		
	}

	@Override
	public TablePage initTablePage(TableSchema schema, byte[] binaryPage, int newPageNumber) throws PageFormatException {


		return new G10TablePage(schema, binaryPage, newPageNumber);
	}

	@Override
	public PageCache createPageCache(PageSize pageSize, int numPages) {

		return new G10PageCache(pageSize, numPages);
	}

	@Override
	public BufferPoolManager createBufferPoolManager(Config config, Logger logger) {

		return new G10BufferPoolManager(config, logger);
	}

	@Override
	public BTreeIndex createBTreeIndex(IndexSchema schema, BufferPoolManager bufferPool, int resourceId) {

		return new G10BTreeIndex(schema, bufferPool, resourceId);
	}

	@Override
	public TableScanOperator createTableScanOperator(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId,
			int[] producedColumnIndexes, LowLevelPredicate[] predicate, int prefetchWindowLength) {
		
		return new G10TableScanOperator(bufferPool, tableManager, resourceId,
				 producedColumnIndexes, predicate, prefetchWindowLength);
	}

	@Override
	public IndexScanOperator createIndexScanOperator(BTreeIndex index, DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded) {
		
		return new G10IndexScanOperator(index, startKey, stopKey, startKeyIncluded, stopKeyIncluded);
	}

	@Override
	public InsertOperator createInsertOperator(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId, BTreeIndex[] indexes,
			int[] columnNumbers, PhysicalPlanOperator child) {
		return new G10InsertOperator(bufferPool, tableManager, resourceId, indexes,
				columnNumbers, child);
	}

	@Override
	public DeleteOperator createDeleteOperator(BufferPoolManager bufferPool, int resourceId, PhysicalPlanOperator child) {
		
		return new G10DeleteOperator(bufferPool, resourceId, child); 
	}

	@Override
	public NestedLoopJoinOperator createNestedLoopJoinOperator(PhysicalPlanOperator outerChild, PhysicalPlanOperator innerChild, JoinPredicate joinPredicate,
			int[] columnMapOuterTuple, int[] columnMapInnerTuple) {
		
		return new G10NestedLoopJoinOperator(outerChild, innerChild, joinPredicate,
												columnMapOuterTuple, columnMapInnerTuple);
	}

	@Override
	public IndexLookupOperator getIndexLookupOperator(BTreeIndex index, DataField equalityLiteral) {

		return new G10IndexLookupOperator(index, equalityLiteral);
	}

	@Override
	public IndexLookupOperator getIndexScanOperatorForBetweenPredicate(BTreeIndex index, DataField lowerBound, boolean lowerIncluded, DataField upperBound,
			boolean upperIncluded) {
		
		return new G10IndexLookupOperator(index, lowerBound, lowerIncluded, upperBound, upperIncluded);
	}

	@Override
	public IndexCorrelatedLookupOperator getIndexCorrelatedScanOperator(BTreeIndex index, int correlatedColumnIndex) {

		return new G10IndexCorrelatedScanOperator(index, correlatedColumnIndex);
	}

	@Override
	public FetchOperator createFetchOperator(PhysicalPlanOperator child, BufferPoolManager bufferPool, int tableResourceId, int[] outputColumnMap) {

		return new G10FetchOperator(child, bufferPool, tableResourceId, outputColumnMap);

	}

	@Override
	public FilterOperator createFilterOperator(PhysicalPlanOperator child, LocalPredicate predicate) {
		
		return new G10FilterOperator(child, predicate);
	}

	@Override
	public FilterCorrelatedOperator createCorrelatedFilterOperator(PhysicalPlanOperator child, JoinPredicate correlatedPredicate) {

		return new G10FilterCorrelatedOperator(child, correlatedPredicate);
	}

	@Override
	public SortOperator createSortOperator(PhysicalPlanOperator child, QueryHeap queryHeap, DataType[] columnTypes, int estimatedCardinality,
			int[] sortColumns, boolean[] columnsAscending) {
		
		return new G10SortOperator(child, queryHeap, columnTypes, estimatedCardinality,	sortColumns, columnsAscending);
	}

	@Override
	public GroupByOperator createGroupByOperator(PhysicalPlanOperator child, int[] groupColumnIndices, int[] aggColumnIndices,
			AggregationType[] aggregateFunctions, DataType[] aggColumnTypes, int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition) {
		
		return new G10GroupByOperator(child, groupColumnIndices, aggColumnIndices,
				aggregateFunctions, aggColumnTypes, groupColumnOutputPositions, aggregateColumnOutputPosition);
			
	}

	@Override
	public MergeJoinOperator createMergeJoinOperator(PhysicalPlanOperator leftChild, PhysicalPlanOperator rightChild, int[] leftJoinColumns,
			int[] rightJoinColumns, int[] columnMapLeftTuple, int[] columnMapRightTuple) {

		return new G10MergeJoinOperator(leftChild, rightChild, leftJoinColumns,
				rightJoinColumns, columnMapLeftTuple, columnMapRightTuple);
	}

	@Override
	public JoinOrderOptimizer createJoinOrderOptimizer(CardinalityEstimator estimator) {
		return new G10JoinOrderOptimizer(estimator);
	}

	@Override
	public CardinalityEstimator createCardinalityEstimator() {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public CostEstimator createCostEstimator(long readCost, long writeCost, long randomReadOverhead, long randomWriteOverhead) {
		return new G10CostEstimator(readCost, writeCost, randomReadOverhead, randomWriteOverhead);
	}

	@Override
	public PhysicalPlanGenerator createPhysicalPlanGenerator(Catalogue catalogue, CardinalityEstimator cardEstimator, CostEstimator costEstimator) {
		return new G10PhysicalPlanGenerator(catalogue, cardEstimator, costEstimator);
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.api.AbstractExtensionFactory#getParser(java.lang.String)
	 */
	@Override
	public SQLParser getParser(String sqlStatement) {
		return null;
	}

	/* Hadoop integration */
	
	@Override
	public Class<? extends TableInputFormat> getTableInputFormat() {
		return G10TableInputFormat.class;
	}

	@Override
	public HadoopOperator<?, ?> createHadoopTableScanOperator(
			DBInstance instance, BulkProcessingOperator child,
			LocalPredicate predicate) {
		
		try {
			return new G10HadoopTableScanOperator( instance, child, predicate);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static HadoopOperator<?, ?> createHadoopGroupByOperator(
			DBInstance instance, BulkProcessingOperator child,
			int[] groupCols,
			int[] aggCols,
			OutputColumn.AggregationType[] aggFunctions,
			DataType[] aggTypes,
			int[] groupColsOutputPos,
			int[] aggColsOutputPos) throws IOException, BufferPoolException, PageExpiredException, PageTupleAccessException, QueryExecutionException {
		
		try {
			return new G10HadoopGroupByOperator( instance, child,
					groupCols,
					aggCols,
					aggFunctions,
					aggTypes,
					groupColsOutputPos,
					aggColsOutputPos);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public HadoopOperator<?,?> createHadoopGroupByOperator(DBInstance instance,
			BulkProcessingOperator child, int[] groupColumnIndices,
			int[] aggColumnIndices, AggregationType[] aggregateFunctions,
			DataType[] aggColumnTypes, int[] groupColumnOutputPositions,
			int[] aggregateColumnOutputPosition) {
		throw new UnsupportedOperationException("Method not yet supported");
	}
}
