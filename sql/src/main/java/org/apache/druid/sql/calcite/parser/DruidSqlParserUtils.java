/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.calcite.parser;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.TimeUnits;
import org.apache.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.filtration.MoveTimeFiltersToIntervals;
import org.apache.druid.sql.calcite.planner.CalcitePlanner;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.base.AbstractInterval;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DruidSqlParserUtils
{

  private static final Logger log = new Logger(DruidSqlParserUtils.class);
  public static final String ALL = "all";

  /**
   * Delegates to {@code convertSqlNodeToGranularity} and converts the exceptions to {@link ParseException}
   * with the underlying message
   */
  public static Granularity convertSqlNodeToGranularityThrowingParseExceptions(SqlNode sqlNode) throws ParseException
  {
    try {
      return convertSqlNodeToGranularity(sqlNode);
    }
    catch (Exception e) {
      log.debug(e, StringUtils.format("Unable to convert %s to a valid granularity.", sqlNode.toString()));
      throw new ParseException(e.getMessage());
    }
  }

  /**
   * This method is used to extract the granularity from a SqlNode representing following function calls:
   * 1. FLOOR(__time TO TimeUnit)
   * 2. TIME_FLOOR(__time, 'PT1H')
   *
   * Validation on the sqlNode is contingent to following conditions:
   * 1. sqlNode is an instance of SqlCall
   * 2. Operator is either one of TIME_FLOOR or FLOOR
   * 3. Number of operands in the call are 2
   * 4. First operand is a SimpleIdentifier representing __time
   * 5. If operator is TIME_FLOOR, the second argument is a literal, and can be converted to the Granularity class
   * 6. If operator is FLOOR, the second argument is a TimeUnit, and can be mapped using {@link TimeUnits}
   *
   * Since it is to be used primarily while parsing the SqlNode, it is wrapped in {@code convertSqlNodeToGranularityThrowingParseExceptions}
   *
   * @param sqlNode SqlNode representing a call to a function
   * @return Granularity as intended by the function call
   * @throws ParseException SqlNode cannot be converted a granularity
   */
  public static Granularity convertSqlNodeToGranularity(SqlNode sqlNode) throws ParseException
  {

    final String genericParseFailedMessageFormatString = "Encountered %s after PARTITIONED BY. "
                                                         + "Expected HOUR, DAY, MONTH, YEAR, ALL TIME, FLOOR function or %s function";

    if (!(sqlNode instanceof SqlCall)) {
      throw new ParseException(StringUtils.format(
          genericParseFailedMessageFormatString,
          sqlNode.toString(),
          TimeFloorOperatorConversion.SQL_FUNCTION_NAME
      ));
    }
    SqlCall sqlCall = (SqlCall) sqlNode;

    String operatorName = sqlCall.getOperator().getName();

    Preconditions.checkArgument(
        "FLOOR".equalsIgnoreCase(operatorName)
        || TimeFloorOperatorConversion.SQL_FUNCTION_NAME.equalsIgnoreCase(operatorName),
        StringUtils.format(
            "PARTITIONED BY clause only supports FLOOR(__time TO <unit> and %s(__time, period) functions",
            TimeFloorOperatorConversion.SQL_FUNCTION_NAME
        )
    );

    List<SqlNode> operandList = sqlCall.getOperandList();
    Preconditions.checkArgument(
        operandList.size() == 2,
        StringUtils.format("%s in PARTITIONED BY clause must have two arguments", operatorName)
    );


    // Check if the first argument passed in the floor function is __time
    SqlNode timeOperandSqlNode = operandList.get(0);
    Preconditions.checkArgument(
        timeOperandSqlNode.getKind().equals(SqlKind.IDENTIFIER),
        StringUtils.format("First argument to %s in PARTITIONED BY clause can only be __time", operatorName)
    );
    SqlIdentifier timeOperandSqlIdentifier = (SqlIdentifier) timeOperandSqlNode;
    Preconditions.checkArgument(
        timeOperandSqlIdentifier.getSimple().equals(ColumnHolder.TIME_COLUMN_NAME),
        StringUtils.format("First argument to %s in PARTITIONED BY clause can only be __time", operatorName)
    );

    // If the floor function is of form TIME_FLOOR(__time, 'PT1H')
    if (operatorName.equalsIgnoreCase(TimeFloorOperatorConversion.SQL_FUNCTION_NAME)) {
      SqlNode granularitySqlNode = operandList.get(1);
      Preconditions.checkArgument(
          granularitySqlNode.getKind().equals(SqlKind.LITERAL),
          "Second argument to TIME_FLOOR in PARTITIONED BY clause must be a period like 'PT1H'"
      );
      String granularityString = SqlLiteral.unchain(granularitySqlNode).toValue();
      Period period;
      try {
        period = new Period(granularityString);
      }
      catch (IllegalArgumentException e) {
        throw new ParseException(StringUtils.format("%s is an invalid period string", granularitySqlNode.toString()));
      }
      return new PeriodGranularity(period, null, null);

    } else if ("FLOOR".equalsIgnoreCase(operatorName)) { // If the floor function is of form FLOOR(__time TO DAY)
      SqlNode granularitySqlNode = operandList.get(1);
      // In future versions of Calcite, this can be checked via
      // granularitySqlNode.getKind().equals(SqlKind.INTERVAL_QUALIFIER)
      Preconditions.checkArgument(
          granularitySqlNode instanceof SqlIntervalQualifier,
          "Second argument to the FLOOR function in PARTITIONED BY clause is not a valid granularity. "
          + "Please refer to the documentation of FLOOR function"
      );
      SqlIntervalQualifier granularityIntervalQualifier = (SqlIntervalQualifier) granularitySqlNode;

      Period period = TimeUnits.toPeriod(granularityIntervalQualifier.timeUnitRange);
      Preconditions.checkNotNull(
          period,
          StringUtils.format(
              "%s is not a valid granularity for ingestion",
              granularityIntervalQualifier.timeUnitRange.toString()
          )
      );
      return new PeriodGranularity(period, null, null);
    }

    // Shouldn't reach here
    throw new ParseException(StringUtils.format(
        genericParseFailedMessageFormatString,
        sqlNode.toString(),
        TimeFloorOperatorConversion.SQL_FUNCTION_NAME
    ));
  }

  /**
   * This method validates and converts a {@link SqlNode} representing a query into an optimized list of intervals to
   * be used in creating an ingestion spec. If the sqlNode is an SqlLiteral of {@link #ALL}, returns a singleton list of
   * "ALL". Otherwise, it converts and optimizes the query using {@link MoveTimeFiltersToIntervals} into a list of
   * intervals which contain all valid values of time as per the query.
   *
   * The following validations are performed
   * 1. Only __time column is present in the query
   * 2. The interval after optimization is not empty
   * 3. The operands in the expression are supported
   * 4. The intervals after adjusting for timezone are aligned with the granularity parameter
   *
   * @param replaceTimeQuery Sql node representing the query
   * @param granularity granularity of the query for validation
   * @param plannerContext planner context
   * @return List of string representation of intervals
   * @throws ValidationException if the SqlNode cannot be converted to a list of intervals
   */
  public static List<String> validateQueryAndConvertToIntervals(
      SqlNode replaceTimeQuery,
      Granularity granularity,
      PlannerContext plannerContext,
      CalcitePlanner planner
  ) throws ValidationException
  {
    if (replaceTimeQuery instanceof SqlLiteral && ALL.equalsIgnoreCase(((SqlLiteral) replaceTimeQuery).toValue())) {
      return ImmutableList.of(ALL);
    }

    Filtration filtration = Filtration.create(convertQueryToDimFilter(replaceTimeQuery, plannerContext, planner));
    filtration = MoveTimeFiltersToIntervals.instance().apply(filtration);
    List<Interval> intervals = filtration.getIntervals();

    if (filtration.getDimFilter() != null) {
      throw new ValidationException("Unsupported operation in OVERWRITE WHERE clause");
    }

    if (intervals.isEmpty()) {
      throw new ValidationException("Intervals for replace are empty");
    }

    for (Interval interval : intervals) {
      DateTime intervalStart = interval.getStart();
      DateTime intervalEnd = interval.getEnd();
      if (!granularity.bucketStart(intervalStart).equals(intervalStart) ||
          !granularity.bucketStart(intervalEnd).equals(intervalEnd)) {
        throw new ValidationException("OVERWRITE WHERE clause contains an interval " + intervals +
                                      " which is not aligned with PARTITIONED BY granularity " + granularity);
      }
    }
    return intervals
        .stream()
        .map(AbstractInterval::toString)
        .collect(Collectors.toList());
  }

  /**
   * Converts the replace time WHERE clause to a filter. It is done by trying to only compile the WHERE clause to a
   * DimFilter using {@link SqlToRelConverter}.
   * @param replaceTimeQuery the replace time WHERE clause
   * @param plannerContext planner context
   * @param planner a CalcitePlanner to plan the replace time WHERE clause
   * @return converted DimFilter from replaceTimeQuery
   * @throws ValidationException if the replace time WHERE clause can't be converted to a DimFilter
   */
  private static DimFilter convertQueryToDimFilter(
      SqlNode replaceTimeQuery,
      PlannerContext plannerContext,
      CalcitePlanner planner
  ) throws ValidationException
  {
    try {
      SqlToRelConverter sqlToRelConverter = planner.getSqlToRelConverter();
      RelDataTypeFactory relDataTypeFactory = planner.getRelDataTypeFactory();
      // convert the expression now by telling that '__time' column is a TIMESTAMP column to help in resolution
      RexNode convertedExpression = sqlToRelConverter.convertExpression(
          replaceTimeQuery,
          ImmutableMap.of(
              ColumnHolder.TIME_COLUMN_NAME,
              new RexInputRef(0, relDataTypeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3))
          )
      );
      RowSignature rowSignature = RowSignature.builder().addTimeColumn().build();
      return Expressions.toFilter(plannerContext, rowSignature, null, convertedExpression);
    }
    catch (Exception e) {
      throw new ValidationException("Invalid OVERWRITE WHERE clause", e);
    }
  }

  /**
   * Extracts and converts the information in the CLUSTERED BY clause to a new SqlOrderBy node.
   *
   * @param query sql query
   * @param clusteredByList List of clustered by columns
   * @return SqlOrderBy node containing the clusteredByList information
   */
  public static SqlOrderBy convertClusterByToOrderBy(SqlNode query, SqlNodeList clusteredByList)
  {
    // If we have a CLUSTERED BY clause, extract the information in that CLUSTERED BY and create a new
    // SqlOrderBy node
    SqlNode offset = null;
    SqlNode fetch = null;

    if (query instanceof SqlOrderBy) {
      SqlOrderBy sqlOrderBy = (SqlOrderBy) query;
      // This represents the underlying query free of OFFSET, FETCH and ORDER BY clauses
      // For a sqlOrderBy.query like "SELECT dim1, sum(dim2) FROM foo OFFSET 10 FETCH 30 ORDER BY dim1 GROUP
      // BY dim1 this would represent the "SELECT dim1, sum(dim2) from foo GROUP BY dim1
      query = sqlOrderBy.query;
      offset = sqlOrderBy.offset;
      fetch = sqlOrderBy.fetch;
    }
    // Creates a new SqlOrderBy query, which may have our CLUSTERED BY overwritten
    return new SqlOrderBy(
        query.getParserPosition(),
        query,
        clusteredByList,
        offset,
        fetch
    );
  }

  /**
   * Throws an IAE with appropriate message if the granularity supplied is not present in
   * {@link org.apache.druid.java.util.common.granularity.Granularities}. It also filters out NONE as it is not a valid
   * granularity that can be supplied in PARTITIONED BY
   */
  public static void throwIfUnsupportedGranularityInPartitionedBy(Granularity granularity)
  {
    if (!GranularityType.isStandard(granularity)) {
      throw new IAE(
          "The granularity specified in PARTITIONED BY is not supported. "
          + "Please use an equivalent of these granularities: %s.",
          Arrays.stream(GranularityType.values())
                .filter(granularityType -> !granularityType.equals(GranularityType.NONE))
                .map(Enum::name)
                .map(StringUtils::toLowerCase)
                .collect(Collectors.joining(", "))
      );
    }
  }
}
