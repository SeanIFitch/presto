/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.protocol.*;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.execution.*;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.*;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.testing.TestingHandle;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.*;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestThriftTaskUpdateRequest
{
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodec<TaskUpdateRequest> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final ThriftCodec<TaskUpdateRequest> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodec<TaskUpdateRequest> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final ThriftCodec<TaskUpdateRequest> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);
    //Dummy values for fake TaskUpdateRequest
    private static final String FAKE_QUERY_ID = "FAKE_QUERY_ID";
    private static final Optional<TransactionId> FAKE_TRANSACTION_ID = Optional.of(TransactionId.create());
    private static final boolean FAKE_CLIENT_TRANSACTION_SUPPORT = false;
    private static final String FAKE_USER = "FAKE_USER";
    private static final Optional<String> FAKE_PRINCIPAL = Optional.of("FAKE_PRINCIPAL");
    private static final Optional<String> FAKE_SOURCE = Optional.of("FAKE_SOURCE");
    private static final Optional<String> FAKE_CATALOG = Optional.of("FAKE_CATALOG");
    private static final Optional<String> FAKE_SCHEMA = Optional.of("FAKE_SCHEMA");
    private static final Optional<String> FAKE_TRACE_TOKEN = Optional.of("FAKE_TRACE_TOKEN");
    private static final TimeZoneKey FAKE_TIME_ZONE_KEY = TimeZoneKey.UTC_KEY;
    private static final Locale FAKE_LOCALE = Locale.ENGLISH;
    private static final Optional<String> FAKE_REMOTE_USER_ADDRESS = Optional.of("FAKE_REMOTE_USER_ADDRESS");
    private static final Optional<String> FAKE_USER_AGENT = Optional.of("FAKE_USER_AGENT");
    private static final Optional<String> FAKE_CLIENT_INFO = Optional.of("FAKE_CLIENT_INFO");
    private static final Set<String> FAKE_CLIENT_TAGS = ImmutableSet.of("FAKE_CLIENT_TAG");
    private static final long FAKE_START_TIME = 124354L;
    private static final Optional<Duration> FAKE_EXECUTION_TIME = Optional.of(new Duration(12434L, TimeUnit.MICROSECONDS));
    private static final Optional<Duration> FAKE_CPU_TIME = Optional.of(new Duration(44279L, TimeUnit.NANOSECONDS));
    private static final Optional<DataSize> FAKE_PEAK_MEMORY = Optional.of(new DataSize(135513L, DataSize.Unit.BYTE));
    private static final Optional<DataSize> FAKE_PEAK_TASK_MEMORY = Optional.of(new DataSize(1313L, DataSize.Unit.MEGABYTE));
    private static final Map<String, String> FAKE_SYSTEM_PROPERTIES = ImmutableMap.of("FAKE_KEY","FAKE_VALUE");
    private static final Map<ConnectorId, Map<String, String>> FAKE_CATALOG_PROPERTIES = ImmutableMap.of(new ConnectorId("FAKE_CATALOG_NAME"), ImmutableMap.of("FAKE_KEY","FAKE_VALUE"));
    private static final Map<String, Map<String, String>> FAKE_UNPROCESSED_CATALOG_PROPERTIES = ImmutableMap.of("FAKE_KEY", ImmutableMap.of("FAKE_KEY","FAKE_VALUE"));
    private static final Map<String, SelectedRole> FAKE_ROLES = ImmutableMap.of("FAKE_KEY", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("FAKE_ROLE")));
    private static final Map<String, String> FAKE_PREPARED_STATEMENTS = ImmutableMap.of("FAKE_KEY","FAKE_VALUE");
    private static final QualifiedObjectName FAKE_QUALIFIED_OBJECT_NAME = new QualifiedObjectName("FAKE_CATALOG_NAME", "FAKE_SCHEMA_NAME", "FAKE_OBJECT_NAME");
    private static final List<TypeVariableConstraint> FAKE_TYPE_VARIABLE_CONSTRAINTS = ImmutableList.of(new TypeVariableConstraint("FAKE_NAME", true, true, "FAKE_VARIADIC_BOUND", true));
    private static final List<LongVariableConstraint> FAKE_LONG_VARIABLE_CONSTRAINTS = ImmutableList.of(new LongVariableConstraint("FAKE_NAME", "FAKE_EXPRESSION"));
    private static final SqlFunctionId FAKE_SQL_FUNCTION_ID = new SqlFunctionId(FAKE_QUALIFIED_OBJECT_NAME, ImmutableList.of(new TypeSignature("FAKE_BASE")));
    private static final String FAKE_NAME = "FAKE_NAME";
    private static final TypeSignature FAKE_TYPE_SIGNATURE = TypeSignature.parseTypeSignature("FAKE_TYPE");
    private static final List<TypeSignature> FAKE_ARGUMENT_TYPES = ImmutableList.of(TypeSignature.parseTypeSignature("FAKE_TYPE"));
    private static final boolean FAKE_VARIABLE_ARITY = true;
    private static final String FAKE_DESCRIPTION = "FAKE_DESCRIPTION";
    private static final String FAKE_BODY = "FAKE_BODY";
    private static final Map<String, String> FAKE_EXTRA_CREDENTIALS = ImmutableMap.of("FAKE_KEY", "FAKE_VALUE");
    private static final Optional<byte[]> FAKE_FRAGMENT = Optional.of(new byte[10]);
    private static final List<TaskSource> FAKE_SOURCES = ImmutableList.of(new TaskSource(new PlanNodeId("FAKE_ID"), ImmutableSet.of(new ScheduledSplit(78124L, new PlanNodeId("FAKE_ID"), new Split(new ConnectorId("FAKE_CATALOG_NAME"), new RemoteTransactionHandle(), new TestingSplit(NodeSelectionStrategy.HARD_AFFINITY, ImmutableList.of(new HostAddress("FAKE_HOST", 638164)))))), true));
    private static final OutputBuffers FAKE_OUTPUT_IDS = new OutputBuffers(OutputBuffers.BufferType.ARBITRARY, 7823L, true, ImmutableMap.of(new OutputBuffers.OutputBufferId(362), 1873));
    private static final PartitioningHandle FAKE_PARTITIONING_HANDLE = new PartitioningHandle(
            Optional.of(new ConnectorId("prism")),
            Optional.of(new ConnectorTransactionHandle() {
                @Override
                public int hashCode()
                {
                    return super.hashCode();
                }

                @Override
                public boolean equals(Object obj)
                {
                    return super.equals(obj);
                }
            }),
            new ConnectorPartitioningHandle() {
                @Override
                public boolean isSingleNode()
                {
                    return false;
                }

                @Override
                public boolean isCoordinatorOnly()
                {
                    return false;
                }
            });
    private static final PlanNode FAKE_PLAN_NODE = new TableScanNode(
            Optional.empty(),
            new PlanNodeId("sourceId"),
            new TableHandle(new ConnectorId("test"), new TestingMetadata.TestingTableHandle(), TestingTransactionHandle.create(), Optional.of(TestingHandle.INSTANCE)),
            ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "column", VARCHAR)),
            ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "column", VARCHAR), new TestingMetadata.TestingColumnHandle("column")),
            TupleDomain.all(),
            TupleDomain.all());
    private static final PlanFragment FAKE_PLAN_FRAGMENT = new PlanFragment(
            new PlanFragmentId(0),
            FAKE_PLAN_NODE,
            ImmutableSet.of(new VariableReferenceExpression(Optional.of(new SourceLocation(133, 135)), "FAKE_NAME", BIGINT)),
            FAKE_PARTITIONING_HANDLE,
            ImmutableList.of(new PlanNodeId("FAKE_PLAN_ID")),
            new PartitioningScheme(Partitioning.create(FAKE_PARTITIONING_HANDLE, ImmutableList.of(new VariableReferenceExpression(Optional.of(new SourceLocation(1, 3)),"FAKE_NAME", BIGINT))), ImmutableList.of(new VariableReferenceExpression(Optional.of(new SourceLocation(0, 1)), "FAKE_NAME", BIGINT))),
            StageExecutionDescriptor.ungroupedExecution(),
            true,
            new StatsAndCosts(ImmutableMap.of(new PlanNodeId("FAKE_PLAN_ID"), new PlanNodeStatsEstimate(1.32, 1.2, true, ImmutableMap.of(new VariableReferenceExpression(Optional.of(new SourceLocation(1, 4)), "FAKE_NAME", BIGINT), new VariableStatsEstimate(1.1, 2.2, 3.3, 4.4, 5.5)))), ImmutableMap.of(new PlanNodeId("FAKE_PLAN_ID"), new PlanCostEstimate(1.1, 2.2, 3.3, 4.4))),
            Optional.of("FAKE_JSON_REPRESENTATION")
            );
    private static final ExecutionWriterTarget FAKE_WRITER_TARGET = new ExecutionWriterTarget.CreateHandle(new OutputTableHandle(new ConnectorId("FAKE_NAME"), new ConnectorTransactionHandle() {}, new ConnectorOutputTableHandle() {}), new SchemaTableName("FAKE_NAME", "FAKE_NAME"));
    private static final AnalyzeTableHandle FAKE_ANALYZE_TABLE_HANDLE = new AnalyzeTableHandle(new ConnectorId("FAKE_NAME"), new RemoteTransactionHandle(), new ConnectorTableHandle() {});
    private static final TableWriteInfo.DeleteScanInfo FAKE_DELETE_SCAN_INFO = new TableWriteInfo.DeleteScanInfo(new PlanNodeId("FAKE_ID"), new TableHandle(new ConnectorId("FAKE_NAME"), new ConnectorTableHandle() {}, new RemoteTransactionHandle(), Optional.of(new ConnectorTableLayoutHandle() {})));
    private static final Optional<TableWriteInfo> FAKE_TABLE_WRITE_INFO = Optional.of(new TableWriteInfo(Optional.of(FAKE_WRITER_TARGET), Optional.of(FAKE_ANALYZE_TABLE_HANDLE), Optional.of(FAKE_DELETE_SCAN_INFO)));
    private TaskUpdateRequest taskUpdateRequest;
    @BeforeMethod
    public void setUp()
    {
        taskUpdateRequest = getTaskUpdateRequest();
    }

    @DataProvider
    public Object[][] codecCombinations()
    {
        return new Object[][] {
                {COMPILER_READ_CODEC, COMPILER_WRITE_CODEC},
                {COMPILER_READ_CODEC, REFLECTION_WRITE_CODEC},
                {REFLECTION_READ_CODEC, COMPILER_WRITE_CODEC},
                {REFLECTION_READ_CODEC, REFLECTION_WRITE_CODEC}
        };
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<TaskUpdateRequest> readCodec, ThriftCodec<TaskUpdateRequest> writeCodec)
            throws Exception
    {
        TaskUpdateRequest taskUpdateRequest = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(taskUpdateRequest);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<TaskUpdateRequest> readCodec, ThriftCodec<TaskUpdateRequest> writeCodec)
            throws Exception
    {
        TaskUpdateRequest taskUpdateRequest = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertSerde(taskUpdateRequest);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<TaskUpdateRequest> readCodec, ThriftCodec<TaskUpdateRequest> writeCodec)
            throws Exception
    {
        TaskUpdateRequest taskUpdateRequest = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertSerde(taskUpdateRequest);
    }

    private void assertSerde(TaskUpdateRequest taskUpdateRequest)
    {

    }

    private TaskUpdateRequest getRoundTripSerialize(ThriftCodec<TaskUpdateRequest> readCodec, ThriftCodec<TaskUpdateRequest> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(taskUpdateRequest, protocol);
        return readCodec.read(protocol);
    }

    private TaskUpdateRequest getTaskUpdateRequest()
    {
        return new TaskUpdateRequest(
                getSession(),
                FAKE_EXTRA_CREDENTIALS,
                FAKE_FRAGMENT,
                FAKE_SOURCES,
                FAKE_OUTPUT_IDS,
                FAKE_TABLE_WRITE_INFO
        );
    }

    private SessionRepresentation getSession() {
        ResourceEstimates resourceEstimates = new ResourceEstimates(
                FAKE_EXECUTION_TIME,
                FAKE_CPU_TIME,
                FAKE_PEAK_MEMORY,
                FAKE_PEAK_TASK_MEMORY
        );
        List<Parameter> parameters = ImmutableList.of(new Parameter(FAKE_NAME, FAKE_TYPE_SIGNATURE));
        RoutineCharacteristics routineCharacteristics = new RoutineCharacteristics(
                Optional.of(RoutineCharacteristics.Language.SQL),
                Optional.of(RoutineCharacteristics.Determinism.DETERMINISTIC),
                Optional.of(RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT)
        );
        Signature signature = new Signature(
                FAKE_QUALIFIED_OBJECT_NAME,
                FunctionKind.AGGREGATE,
                FAKE_TYPE_VARIABLE_CONSTRAINTS,
                FAKE_LONG_VARIABLE_CONSTRAINTS,
                FAKE_TYPE_SIGNATURE,
                FAKE_ARGUMENT_TYPES,
                FAKE_VARIABLE_ARITY
        );
        SqlInvokedFunction sqlInvokedFunction = new SqlInvokedFunction(
                parameters,
                FAKE_DESCRIPTION,
                routineCharacteristics,
                FAKE_BODY,
                signature,
                FAKE_SQL_FUNCTION_ID
        );
        Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions = ImmutableMap.of(FAKE_SQL_FUNCTION_ID, sqlInvokedFunction);
        return new SessionRepresentation(
                FAKE_QUERY_ID,
                FAKE_TRANSACTION_ID,
                FAKE_CLIENT_TRANSACTION_SUPPORT,
                FAKE_USER,
                FAKE_PRINCIPAL,
                FAKE_SOURCE,
                FAKE_CATALOG,
                FAKE_SCHEMA,
                FAKE_TRACE_TOKEN,
                FAKE_TIME_ZONE_KEY,
                FAKE_LOCALE,
                FAKE_REMOTE_USER_ADDRESS,
                FAKE_USER_AGENT,
                FAKE_CLIENT_INFO,
                FAKE_CLIENT_TAGS,
                resourceEstimates,
                FAKE_START_TIME,
                FAKE_SYSTEM_PROPERTIES,
                FAKE_CATALOG_PROPERTIES,
                FAKE_UNPROCESSED_CATALOG_PROPERTIES,
                FAKE_ROLES,
                FAKE_PREPARED_STATEMENTS,
                sessionFunctions
        );
    }
}
