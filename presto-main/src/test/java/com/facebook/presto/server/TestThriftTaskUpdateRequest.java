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
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.*;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.StreamingSubPlan;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.util.Failures;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.swing.text.html.Option;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.facebook.presto.spi.StandardErrorCode.*;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestThriftTaskUpdateRequest
{
    /*private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodec<TaskStatus> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(TaskStatus.class);
    private static final ThriftCodec<TaskStatus> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(TaskStatus.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodec<TaskStatus> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(TaskStatus.class);
    private static final ThriftCodec<TaskStatus> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(TaskStatus.class);
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
    private static Optional<Duration> FAKE_EXECUTION_TIME = Optional.of(new Duration(12434L, TimeUnit.MICROSECONDS));
    private static Optional<Duration> FAKE_CPU_TIME = Optional.of(new Duration(44279L, TimeUnit.NANOSECONDS));
    private static Optional<DataSize> FAKE_PEAK_MEMORY = Optional.of(new DataSize(135513L, DataSize.Unit.BYTE));
    private static Optional<DataSize> FAKE_PEAK_TASK_MEMORY = Optional.of(new DataSize(1313L, DataSize.Unit.MEGABYTE));
    private static final Map<String, String> FAKE_SYSTEM_PROPERTIES = ImmutableMap.of("FAKE_KEY","FAKE_VALUE");
    private static final Map<ConnectorId, Map<String, String>> FAKE_CATALOG_PROPERTIES = ImmutableMap.of(new ConnectorId("FAKE_CATALOG_NAME"), ImmutableMap.of("FAKE_KEY","FAKE_VALUE"));
    private static final Map<String, Map<String, String>> FAKE_UNPROCESSED_CATALOG_PROPERTIES = ImmutableMap.of("FAKE_KEY", ImmutableMap.of("FAKE_KEY","FAKE_VALUE"));
    private static final Map<String, SelectedRole> FAKE_ROLES = ImmutableMap.of("FAKE_KEY", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("FAKE_ROLE")));
    private static final Map<String, String> FAKE_PREPARED_STATEMENTS = ImmutableMap.of("FAKE_KEY","FAKE_VALUE");
    private static final QualifiedObjectName FAKE_QUALIFIED_OBJECT_NAME = new QualifiedObjectName("FAKE_CATALOG_NAME", "FAKE_SCHEMA_NAME", "FAKE_OBJECT_NAME");
    private static final SqlFunctionId FAKE_SQL_FUNCTION_ID = new SqlFunctionId(FAKE_QUALIFIED_OBJECT_NAME, ImmutableList.of(new TypeSignature("FAKE_BASE")));
    private static final String FAKE_NAME = "FAKE_NAME";
    private static final TypeSignature FAKE_TYPE_SIGNATURE = TypeSignature.parseTypeSignature("FAKE_TYPE");
    private static final String FAKE_DESCRIPTION = "FAKE_DESCRIPTION";
    private static final String FAKE_BODY = "FAKE_BODY";
    private static final Map<String, String> FAKE_EXTRA_CREDENTIALS = ImmutableMap.of("FAKE_KEY", "FAKE_VALUE");
    private static final Optional<byte[]> FAKE_FRAGMENT = Optional.of(new byte[10]);
    private static final List<TaskSource> FAKE_SOURCES = ImmutableList.of(new TaskSource(new PlanNodeId("FAKE_ID"), ImmutableSet.of(new ScheduledSplit(78124L, new PlanNodeId("FAKE_ID"), new Split(new ConnectorId("FAKE_CATALOG_NAME"), new RemoteTransactionHandle(), new TestingSplit(NodeSelectionStrategy.HARD_AFFINITY, ImmutableList.of(new HostAddress("FAKE_HOST", 638164)))))), true));
    private static final OutputBuffers FAKE_OUTPUT_IDS = new OutputBuffers(OutputBuffers.BufferType.ARBITRARY, 7823L, true, ImmutableMap.of(new OutputBuffers.OutputBufferId(362), 1873));
    private static final Optional<TableWriteInfo> FAKE_TABLE_WRITE_INFO = Optional.of(TableWriteInfo.createTableWriteInfo(new StreamingSubPlan(new PlanFragment(), ImmutableList.of())));
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
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<TaskStatus> readCodec, ThriftCodec<TaskStatus> writeCodec)
            throws Exception
    {
        TaskStatus taskStatus = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(taskStatus);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<TaskStatus> readCodec, ThriftCodec<TaskStatus> writeCodec)
            throws Exception
    {
        TaskStatus taskStatus = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertSerde(taskStatus);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<TaskStatus> readCodec, ThriftCodec<TaskStatus> writeCodec)
            throws Exception
    {
        TaskStatus taskStatus = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertSerde(taskStatus);
    }

    private void assertSerde(TaskStatus taskStatus)
    {
        assertEquals(taskStatus.getTaskInstanceIdLeastSignificantBits(), 123L);
        assertEquals(taskStatus.getTaskInstanceIdMostSignificantBits(), 456L);
        assertEquals(taskStatus.getVersion(), 789L);
        assertEquals(taskStatus.getState(), TaskState.RUNNING);
        assertEquals(taskStatus.getSelf(), SELF_URI);
        assertEquals(taskStatus.getCompletedDriverGroups(), LIFESPANS);
        assertEquals(taskStatus.getQueuedPartitionedDrivers(), QUEUED_PARTITIONED_DRIVERS);
        assertEquals(taskStatus.getQueuedPartitionedSplitsWeight(), QUEUED_PARTITIONED_WEIGHT);
        assertEquals(taskStatus.getRunningPartitionedDrivers(), RUNNING_PARTITIONED_DRIVERS);
        assertEquals(taskStatus.getRunningPartitionedSplitsWeight(), RUNNING_PARTITIONED_WEIGHT);
        assertEquals(taskStatus.getOutputBufferUtilization(), OUTPUT_BUFFER_UTILIZATION);
        assertEquals(taskStatus.isOutputBufferOverutilized(), OUTPUT_BUFFER_OVERUTILIZED);
        assertEquals(taskStatus.getPhysicalWrittenDataSizeInBytes(), PHYSICAL_WRITTEN_DATA_SIZE_IN_BYTES);
        assertEquals(taskStatus.getSystemMemoryReservationInBytes(), SYSTEM_MEMORY_RESERVATION_IN_BYTES);
        assertEquals(taskStatus.getPeakNodeTotalMemoryReservationInBytes(), PEAK_NODE_TOTAL_MEMORY_RESERVATION_IN_BYTES);
        assertEquals(taskStatus.getFullGcCount(), FULL_GC_COUNT);
        assertEquals(taskStatus.getFullGcTimeInMillis(), FULL_GC_TIME_IN_MILLIS);
        assertEquals(taskStatus.getTotalCpuTimeInNanos(), TOTAL_CPU_TIME_IN_NANOS);
        assertEquals(taskStatus.getTaskAgeInMillis(), TASK_AGE);

        List<ExecutionFailureInfo> failures = taskStatus.getFailures();
        assertEquals(failures.size(), 3);

        ExecutionFailureInfo firstFailure = failures.get(0);
        assertEquals(firstFailure.getType(), IOException.class.getName());
        assertEquals(firstFailure.getMessage(), "Remote call timed out");
        assertEquals(firstFailure.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        List<ExecutionFailureInfo> suppressedFailures = firstFailure.getSuppressed();
        assertEquals(suppressedFailures.size(), 1);
        ExecutionFailureInfo suppressedFailure = suppressedFailures.get(0);
        assertEquals(suppressedFailure.getType(), IOException.class.getName());
        assertEquals(suppressedFailure.getMessage(), "Thrift call timed out");
        assertEquals(suppressedFailure.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());

        ExecutionFailureInfo secondFailure = failures.get(1);
        assertEquals(secondFailure.getType(), PrestoTransportException.class.getName());
        assertEquals(secondFailure.getMessage(), "Too many requests failed");
        assertEquals(secondFailure.getRemoteHost(), REMOTE_HOST);
        assertEquals(secondFailure.getErrorCode(), TOO_MANY_REQUESTS_FAILED.toErrorCode());
        ExecutionFailureInfo cause = secondFailure.getCause();
        assertEquals(cause.getType(), PrestoException.class.getName());
        assertEquals(cause.getMessage(), "Remote Task Error");
        assertEquals(cause.getErrorCode(), REMOTE_TASK_ERROR.toErrorCode());

        ExecutionFailureInfo thirdFailure = failures.get(2);
        assertEquals(thirdFailure.getType(), ParsingException.class.getName());
        assertEquals(thirdFailure.getErrorCode(), SYNTAX_ERROR.toErrorCode());
        assertEquals(thirdFailure.getErrorLocation().getLineNumber(), 100);
        assertEquals(thirdFailure.getErrorLocation().getColumnNumber(), 2);
    }

    private TaskStatus getRoundTripSerialize(ThriftCodec<TaskStatus> readCodec, ThriftCodec<TaskStatus> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(taskStatus, protocol);
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

    private SessionRepresentation getSession()
    {
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
                //CONTINUE HERE
        );
        SqlInvokedFunction sqlInvokedFunction = new SqlInvokedFunction(
                parameters,
                FAKE_DESCRIPTION,
                routineCharacteristics,
                FAKE_BODY,
                //CONTINUE HERE WITH SIGNATURE ETC
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
    }*/
}
