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
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.prepare.BaseDruidSqlValidator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.ParameterScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.sql.calcite.planner.CalcitePlanner;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * Extends the 'replace' call to hold custom parameters specific to Druid i.e. PARTITIONED BY and the PARTITION SPECS
 * This class extends the {@link SqlInsert} so that this SqlNode can be used in
 * {@link org.apache.calcite.sql2rel.SqlToRelConverter} for getting converted into RelNode, and further processing
 */
public class DruidSqlReplace extends SqlInsert
{
  public static final String SQL_REPLACE_TIME_CHUNKS = "sqlReplaceTimeChunks";

  public static final SqlOperator OPERATOR = new SqlSpecialOperator("REPLACE", SqlKind.OTHER);

  private final Granularity partitionedBy;

  // Used in the unparse function to generate the original query since we convert the string to an enum
  private final String partitionedByStringForUnparse;

  private SqlNode replaceTimeQuery;

  @Nullable
  private final SqlNodeList clusteredBy;

  /**
   * While partitionedBy and partitionedByStringForUnparse can be null as arguments to the constructor, this is
   * disallowed (semantically) and the constructor performs checks to ensure that. This helps in producing friendly
   * errors when the PARTITIONED BY custom clause is not present, and keeps its error separate from JavaCC/Calcite's
   * custom errors which can be cryptic when someone accidentally forgets to explicitly specify the PARTITIONED BY clause
   */
  public DruidSqlReplace(
      @Nonnull SqlInsert insertNode,
      @Nullable Granularity partitionedBy,
      @Nullable String partitionedByStringForUnparse,
      @Nullable SqlNodeList clusteredBy,
      @Nullable SqlNode replaceTimeQuery
  ) throws ParseException
  {
    super(
        insertNode.getParserPosition(),
        (SqlNodeList) insertNode.getOperandList().get(0), // No better getter to extract this
        insertNode.getTargetTable(),
        insertNode.getSource(),
        insertNode.getTargetColumnList()
    );
    if (replaceTimeQuery == null) {
      throw new ParseException("Missing time chunk information in OVERWRITE clause for REPLACE, set it to OVERWRITE WHERE <__time based condition> or set it to overwrite the entire table with OVERWRITE ALL.");
    }
    if (partitionedBy == null) {
      throw new ParseException("REPLACE statements must specify PARTITIONED BY clause explicitly");
    }
    this.partitionedBy = partitionedBy;

    this.partitionedByStringForUnparse = Preconditions.checkNotNull(partitionedByStringForUnparse);

    this.replaceTimeQuery = replaceTimeQuery;

    this.clusteredBy = clusteredBy;
  }

  public SqlNode getReplaceTimeQuery()
  {
    return replaceTimeQuery;
  }

  public Granularity getPartitionedBy()
  {
    return partitionedBy;
  }

  @Nullable
  public SqlNodeList getClusteredBy()
  {
    return clusteredBy;
  }

  @Nonnull
  @Override
  public SqlOperator getOperator()
  {
    return OPERATOR;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.sep("REPLACE INTO");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    getTargetTable().unparse(writer, opLeft, opRight);

    if (getTargetColumnList() != null) {
      getTargetColumnList().unparse(writer, opLeft, opRight);
    }
    writer.newlineAndIndent();

    writer.keyword("OVERWRITE");
    if (replaceTimeQuery instanceof SqlLiteral) {
      writer.keyword("ALL");
    } else {
      replaceTimeQuery.unparse(writer, leftPrec, rightPrec);
    }
    writer.newlineAndIndent();

    getSource().unparse(writer, 0, 0);
    writer.newlineAndIndent();

    writer.keyword("PARTITIONED BY");
    writer.keyword(partitionedByStringForUnparse);

    if (getClusteredBy() != null) {
      writer.keyword("CLUSTERED BY");
      SqlWriter.Frame frame = writer.startList("", "");
      for (SqlNode clusterByOpts : getClusteredBy().getList()) {
        clusterByOpts.unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(frame);
    }
  }

  /**
   * Currently, this only validates the 'WHERE' clause for the REPLACE statement. It is so because as of now, the INSERT
   * nodes are not validated by the calcite side of the planner. When that happens, we should also validate the INSERT
   * node. The current validation is being done by
   * {@link org.apache.druid.sql.calcite.planner.DruidPlanner.ParsedNodes#convertReplaceWhereClause(PlannerContext, CalcitePlanner)}
   * to convert 'WHERE' clause to a filter.
   * @param validator validator used for the 'WHERE' clause validation
   * @param scope scope of the SqlNode
   */
  @Override
  public void validate(SqlValidator validator, SqlValidatorScope scope)
  {
    assert validator instanceof BaseDruidSqlValidator;
    BaseDruidSqlValidator baseDruidSqlValidator = (BaseDruidSqlValidator) validator;
    replaceTimeQuery = baseDruidSqlValidator.performUnconditionalRewrites(replaceTimeQuery, false);
    replaceTimeQuery.validate(validator, new DruidParameterScope(
        baseDruidSqlValidator,
        ImmutableMap.of(ColumnHolder.TIME_COLUMN_NAME, validator.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP))
    ));
  }

  /**
   * An extension of {@link ParameterScope} which contains a fix for custom column resolution. The fix is present in a
   * newer version of Calcite (1.22.0, issue : https://issues.apache.org/jira/browse/CALCITE-3476)
   */
  private static class DruidParameterScope extends ParameterScope
  {
    private final Map<String, RelDataType> nameToTypeMap;

    public DruidParameterScope(
        SqlValidatorImpl validator,
        Map<String, RelDataType> nameToTypeMap
    )
    {
      super(validator, nameToTypeMap);
      this.nameToTypeMap = nameToTypeMap;
    }

    @Override
    public RelDataType resolveColumn(String name, SqlNode ctx)
    {
      return nameToTypeMap.get(name);
    }
  }
}
