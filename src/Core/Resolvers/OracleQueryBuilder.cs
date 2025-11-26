// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Data.Common;
using System.Text;
using Azure.DataApiBuilder.Config.ObjectModel;
using Azure.DataApiBuilder.Core.Models;
using Oracle.ManagedDataAccess.Client;

namespace Azure.DataApiBuilder.Core.Resolvers
{
    /// <summary>
    /// Oracle-specific query builder supporting Oracle 23ai features including JSON and Vector columns.
    /// </summary>
    public class OracleQueryBuilder : BaseSqlQueryBuilder, IQueryBuilder
    {
        private static readonly DbCommandBuilder _builder = new OracleCommandBuilder();

        /// <summary>
        /// Oracle uses double quotes for identifier quoting.
        /// </summary>
        public override string QuoteIdentifier(string ident)
        {
            return _builder.QuoteIdentifier(ident);
        }

        /// <inheritdoc />
        public string Build(SqlQueryStructure structure)
        {
            string fromSql = $"{QuoteIdentifier(structure.DatabaseObject.SchemaName)}.{QuoteIdentifier(structure.DatabaseObject.Name)} " +
                             $"{QuoteIdentifier(structure.SourceAlias)}{Build(structure.Joins)}";

            // Oracle uses OUTER APPLY for lateral joins (similar to SQL Server)
            fromSql += string.Join("", structure.JoinQueries.Select(x =>
                $" OUTER APPLY ({Build(x.Value)}) {QuoteIdentifier(x.Key)}"));

            string predicates = JoinPredicateStrings(
                                    structure.GetDbPolicyForOperation(EntityActionOperation.Read),
                                    structure.FilterPredicates,
                                    Build(structure.Predicates),
                                    Build(structure.PaginationMetadata.PaginationPredicate));

            // Build aggregation columns
            string aggregations = BuildAggregationColumns(structure);

            // Build GROUP BY clause
            string groupBy = BuildGroupBy(structure);

            // Build HAVING clause
            string having = BuildHaving(structure);

            // Build ORDER BY clause
            string orderBy = BuildOrderBy(structure);

            // Oracle uses FETCH FIRST/OFFSET instead of LIMIT/OFFSET
            StringBuilder queryBuilder = new();
            queryBuilder.Append($"SELECT {MakeSelectColumns(structure)}{aggregations}")
                .Append($" FROM {fromSql}")
                .Append($" WHERE {predicates}")
                .Append(groupBy)
                .Append(having)
                .Append(orderBy)
                .Append($" OFFSET 0 ROWS FETCH FIRST {structure.Limit()} ROWS ONLY");

            string query = queryBuilder.ToString();
            string subqueryName = QuoteIdentifier($"subq{structure.Counter.Next()}");

            // **FIX: Build explicit column list for JSON serialization**
            List<string> jsonKeyValuePairs = new();

            // Add regular columns
            foreach (LabelledColumn column in structure.Columns)
            {
                jsonKeyValuePairs.Add($"'{column.Label}' VALUE {QuoteIdentifier(column.Label)}");
            }

            // Add aggregation columns
            if (structure.GroupByMetadata.Aggregations.Count > 0)
            {
                foreach (AggregationOperation aggregation in structure.GroupByMetadata.Aggregations)
                {
                    string alias = aggregation.Column.OperationAlias;
                    jsonKeyValuePairs.Add($"'{alias}' VALUE {QuoteIdentifier(alias)}");
                }
            }

            string jsonColumnList = string.Join(", ", jsonKeyValuePairs);

            StringBuilder result = new();
            if (structure.IsListQuery)
            {
                // Oracle 23ai JSON_ARRAYAGG for array aggregation with explicit columns
                result.Append($"SELECT JSON_ARRAYAGG(JSON_OBJECT({jsonColumnList}) RETURNING CLOB) ");
            }
            else
            {
                // Use explicit columns instead of * for proper aggregation support
                result.Append($"SELECT JSON_OBJECT({jsonColumnList}) ");
            }

            result.Append($"AS {QuoteIdentifier(SqlQueryStructure.DATA_IDENT)} FROM ( ");
            result.Append(query);
            result.Append($" ) {subqueryName}");

            return result.ToString();
        }

        /// <summary>
        /// Build the aggregation columns needed to append to the main query
        /// </summary>
        /// <param name="structure">Sql query structure to build query on</param>
        /// <returns>SQL query with aggregation columns</returns>
        private string BuildAggregationColumns(SqlQueryStructure structure)
        {
            string aggregations = string.Empty;
            if (structure.GroupByMetadata.Aggregations.Count > 0)
            {
                if (structure.Columns.Any())
                {
                    aggregations = $",{BuildAggregationColumns(structure.GroupByMetadata)}";
                }
                else
                {
                    aggregations = $"{BuildAggregationColumns(structure.GroupByMetadata)}";
                }
            }

            return aggregations;
        }

        /// <summary>
        /// Build the aggregation columns needed to append to the main query
        /// </summary>
        /// <param name="metadata">GroupByMetadata</param>
        /// <returns>SQL query with aggregation columns</returns>
        private string BuildAggregationColumns(GroupByMetadata metadata)
        {
            return string.Join(", ", metadata.Aggregations.Select(aggregation => Build(aggregation.Column, useAlias: true)));
        }

        /// <summary>
        /// Build the Group By Clause needed to append to the main query
        /// </summary>
        /// <param name="structure">Sql query structure to build query on</param>
        /// <returns>SQL query with group-by clause</returns>
        private string BuildGroupBy(SqlQueryStructure structure)
        {
            // Add GROUP BY clause if there are any group by columns
            if (structure.GroupByMetadata.Fields.Any())
            {
                return $" GROUP BY {string.Join(", ", structure.GroupByMetadata.Fields.Values.Select(c => Build(c)))}";
            }

            return string.Empty;
        }

        /// <summary>
        /// Build the Having clause needed to append to the main query
        /// </summary>
        /// <param name="structure">Sql query structure to build query on</param>
        /// <returns>SQL query with having clause</returns>
        private string BuildHaving(SqlQueryStructure structure)
        {
            if (structure.GroupByMetadata.Aggregations.Count > 0)
            {
                List<Predicate>? havingPredicates = structure.GroupByMetadata.Aggregations
                      .SelectMany(aggregation => aggregation.HavingPredicates ?? new List<Predicate>())
                      .ToList();

                if (havingPredicates.Any())
                {
                    return $" HAVING {Build(havingPredicates)}";
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Build the Order By clause needed to append to the main query
        /// </summary>
        /// <param name="structure">Sql query structure to build query on</param>
        /// <returns>SQL query with order-by clause</returns>
        private string BuildOrderBy(SqlQueryStructure structure)
        {
            if (structure.OrderByColumns.Any())
            {
                return $" ORDER BY {Build(structure.OrderByColumns)}";
            }

            return string.Empty;
        }

        /// <inheritdoc />
        public string Build(SqlInsertStructure structure)
        {
            StringBuilder insertQuery = new();
            insertQuery.Append($"INSERT INTO {QuoteIdentifier(structure.DatabaseObject.SchemaName)}.{QuoteIdentifier(structure.DatabaseObject.Name)} ");
            
            if (structure.InsertColumns.Any())
            {
                insertQuery.Append($"({Build(structure.InsertColumns)}) ");
                insertQuery.Append($"VALUES ({string.Join(", ", structure.Values)}) ");
            }
            else
            {
                // Oracle doesn't support DEFAULT VALUES in INSERT, use a different syntax
                insertQuery.Append($"({Build(structure.OutputColumns.Select(c => c.ColumnName).ToList())}) ");
                insertQuery.Append("VALUES (");
                insertQuery.Append(string.Join(", ", structure.OutputColumns.Select(_ => "DEFAULT")));
                insertQuery.Append(") ");
            }

            // FIX: Oracle RETURNING clause - use only column names without table qualification or aliases
            if (structure.OutputColumns.Any())
            {
                // Build list of column names only (no table prefix, no aliases)
                List<string> returningColumns = structure.OutputColumns
                    .Select(c => QuoteIdentifier(c.ColumnName))
                    .ToList();
                
                // Build list of output parameters
                List<string> intoParams = structure.OutputColumns
                    .Select(c => $":out_{c.ColumnName}")
                    .ToList();

                insertQuery.Append($"RETURNING {string.Join(", ", returningColumns)} ");
                insertQuery.Append($"INTO {string.Join(", ", intoParams)}");
            }

            return insertQuery.ToString();
        }

        /// <inheritdoc />
        public string Build(SqlUpdateStructure structure)
        {
            string predicates = JoinPredicateStrings(
                                   structure.GetDbPolicyForOperation(EntityActionOperation.Update),
                                   Build(structure.Predicates));

            StringBuilder updateQuery = new();
            updateQuery.Append($"UPDATE {QuoteIdentifier(structure.DatabaseObject.SchemaName)}.{QuoteIdentifier(structure.DatabaseObject.Name)} ");
            updateQuery.Append($"SET {Build(structure.UpdateOperations, ", ")} ");
            updateQuery.Append($"WHERE {predicates} ");
            
            // FIX: Oracle RETURNING clause - use only column names without table qualification or aliases
            if (structure.OutputColumns.Any())
            {
                List<string> returningColumns = structure.OutputColumns
                    .Select(c => QuoteIdentifier(c.ColumnName))
                    .ToList();
                
                List<string> intoParams = structure.OutputColumns
                    .Select(c => $":out_{c.ColumnName}")
                    .ToList();

                updateQuery.Append($"RETURNING {string.Join(", ", returningColumns)} ");
                updateQuery.Append($"INTO {string.Join(", ", intoParams)}");
            }

            return updateQuery.ToString();
        }

        /// <inheritdoc />
        public string Build(SqlDeleteStructure structure)
        {
            string predicates = JoinPredicateStrings(
                       structure.GetDbPolicyForOperation(EntityActionOperation.Delete),
                       Build(structure.Predicates));

            return $"DELETE FROM {QuoteIdentifier(structure.DatabaseObject.SchemaName)}.{QuoteIdentifier(structure.DatabaseObject.Name)} " +
                    $"WHERE {predicates}";
        }

        /// <inheritdoc />
        public string Build(SqlExecuteStructure structure)
        {
            // Oracle stored procedure execution using anonymous PL/SQL block
            StringBuilder procCall = new("BEGIN ");
            procCall.Append($"{QuoteIdentifier(structure.DatabaseObject.SchemaName)}.{QuoteIdentifier(structure.DatabaseObject.Name)}(");
            
            // Add parameters
            List<string> paramNames = new();
            foreach (KeyValuePair<string, object> param in structure.ProcedureParameters)
            {
                paramNames.Add($":{param.Key}");
            }

            procCall.Append(string.Join(", ", paramNames));
            procCall.Append("); END;");
            
            return procCall.ToString();
        }

        /// <inheritdoc />
        public string Build(SqlUpsertQueryStructure structure)
        {
            // Oracle 23ai supports MERGE with RETURNING clause
            StringBuilder mergeQuery = new();
            
            string tableName = $"{QuoteIdentifier(structure.DatabaseObject.SchemaName)}.{QuoteIdentifier(structure.DatabaseObject.Name)}";

            mergeQuery.AppendLine("BEGIN");
           mergeQuery.Append($"MERGE INTO {tableName} target ");
            
            // Build the USING clause with a dual-based subquery
            mergeQuery.Append("USING (SELECT ");
            List<string> sourceSelects = new();
            for (int i = 0; i < structure.InsertColumns.Count; i++)
            {
                sourceSelects.Add($"{structure.Values[i]} AS {QuoteIdentifier(structure.InsertColumns[i])}");
            }

            mergeQuery.Append(string.Join(", ", sourceSelects));
            mergeQuery.Append(" FROM DUAL) source ");
            
            // Build the ON clause using target and source aliases
            List<string> onConditions = new();
            foreach (Predicate predicate in structure.Predicates)
            {
                if (predicate.Left?.AsColumn() is Column leftColumn)
                {
                    string targetColumn = $"target.{QuoteIdentifier(leftColumn.ColumnName)}";
                    string sourceColumn = $"source.{QuoteIdentifier(leftColumn.ColumnName)}";
                    onConditions.Add($"{targetColumn} = {sourceColumn}");
                }
            }
            
            mergeQuery.Append($"ON ({string.Join(" AND ", onConditions)}) ");
            
            // WHEN MATCHED clause (update)
            if (structure.UpdateOperations.Any())
            {
                mergeQuery.Append("WHEN MATCHED THEN UPDATE SET ");
                
                List<string> updateOps = new();
                foreach (Predicate updateOp in structure.UpdateOperations)
                {
                    if (updateOp.Left?.AsColumn() is Column col)
                    {
                        // Don't use target alias in SET clause for MERGE
                        string columnName = QuoteIdentifier(col.ColumnName);
                        
                        // Extract the parameter name from the Right PredicateOperand
                        string? paramName = updateOp.Right.AsString();
                        
                        if (paramName != null)
                        {
                            updateOps.Add($"{columnName} = {paramName}");
                        }
                    }
                }
                
                mergeQuery.Append(string.Join(", ", updateOps));
                
                // Add update policy if exists
                string? updatePolicy = structure.GetDbPolicyForOperation(EntityActionOperation.Update);
                if (!string.IsNullOrEmpty(updatePolicy))
                {
                    mergeQuery.Append($" WHERE {updatePolicy}");
                }

                mergeQuery.Append(" ");
            }
            
            // WHEN NOT MATCHED clause (insert)
            mergeQuery.Append("WHEN NOT MATCHED THEN INSERT (");
            mergeQuery.Append(Build(structure.InsertColumns));
            mergeQuery.Append(") VALUES (");
            
            // Use source alias for INSERT values
            List<string> insertValues = structure.InsertColumns
                .Select(col => $"source.{QuoteIdentifier(col)}")
                .ToList();
            mergeQuery.Append(string.Join(", ", insertValues));
            mergeQuery.Append(") ");
            
            // Oracle 23ai: Add RETURNING clause for MERGE
            // This returns the affected row regardless of whether it was inserted or updated
            if (structure.OutputColumns.Any())
            {
                // Build list of column names only (no table prefix, no aliases)
                List<string> returningColumns = structure.OutputColumns
                    .Select(c => QuoteIdentifier(c.ColumnName))
                    .ToList();
                
                // Build list of output parameters
                List<string> intoParams = structure.OutputColumns
                    .Select(c => $":out_{c.ColumnName}")
                    .ToList();

                mergeQuery.Append($"RETURNING {string.Join(", ", returningColumns)} ");
                mergeQuery.Append($"INTO {string.Join(", ", intoParams)}");
            }

            mergeQuery.AppendLine(";");
            mergeQuery.Append("END;");
            return mergeQuery.ToString();
        }

        /// <inheritdoc />
        public override string BuildForeignKeyInfoQuery(int numberOfParameters)
        {
            string[] schemaNameParams = CreateParams("schemaName", numberOfParameters);
            string[] tableNameParams = CreateParams("tableName", numberOfParameters);
            string schemaParamsForInClause = string.Join(", :", schemaNameParams);
            string tableNameParamsForInClause = string.Join(", :", tableNameParams);

            // Oracle uses ALL_CONSTRAINTS and ALL_CONS_COLUMNS views
            string foreignKeyQuery = $@"
SELECT 
    fk.CONSTRAINT_NAME AS {QuoteIdentifier("ForeignKeyDefinition")},
    fk.OWNER AS {QuoteIdentifier("ReferencingSchemaName")},
    fk.TABLE_NAME AS {QuoteIdentifier("ReferencingSourceDefinition")},
    fk_cols.COLUMN_NAME AS {QuoteIdentifier("ReferencingColumns")},
    pk.OWNER AS {QuoteIdentifier("ReferencedSchemaName")},
    pk.TABLE_NAME AS {QuoteIdentifier("ReferencedSourceDefinition")},
    pk_cols.COLUMN_NAME AS {QuoteIdentifier("ReferencedColumns")}
FROM 
    ALL_CONSTRAINTS fk
    INNER JOIN ALL_CONS_COLUMNS fk_cols 
        ON fk.CONSTRAINT_NAME = fk_cols.CONSTRAINT_NAME 
        AND fk.OWNER = fk_cols.OWNER
    INNER JOIN ALL_CONSTRAINTS pk 
        ON fk.R_CONSTRAINT_NAME = pk.CONSTRAINT_NAME 
        AND fk.R_OWNER = pk.OWNER
    INNER JOIN ALL_CONS_COLUMNS pk_cols 
        ON pk.CONSTRAINT_NAME = pk_cols.CONSTRAINT_NAME 
        AND pk.OWNER = pk_cols.OWNER
        AND fk_cols.POSITION = pk_cols.POSITION
WHERE 
    fk.CONSTRAINT_TYPE = 'R'
    AND fk.OWNER IN (:{schemaParamsForInClause})
    AND fk.TABLE_NAME IN (:{tableNameParamsForInClause})
ORDER BY 
    fk.CONSTRAINT_NAME, fk_cols.POSITION";

            return foreignKeyQuery;
        }

        /// <inheritdoc />
        public string BuildQueryToGetReadOnlyColumns(string schemaParamName, string tableParamName)
        {
            string query = $@"
SELECT 
    COLUMN_NAME
FROM 
    ALL_TAB_COLS
WHERE 
    OWNER = {schemaParamName}
    AND TABLE_NAME = {tableParamName}
    AND VIRTUAL_COLUMN = 'YES'";
            
            return query;
        }

        /// <inheritdoc />
        public string BuildStoredProcedureResultDetailsQuery(string databaseObjectName)
        {
            // Oracle doesn't provide metadata for procedure result sets as easily as SQL Server
            // This would need to be implemented based on specific Oracle procedure metadata views
            // For now, throw NotImplementedException as this requires runtime execution analysis
            throw new NotImplementedException(
                "Stored procedure result metadata querying for Oracle requires runtime execution analysis.");
        }

        /// <inheritdoc />
        public string BuildFetchEnabledTriggersQuery()
        {
            // Oracle trigger metadata query
            // This returns enabled triggers for INSERT and UPDATE operations
            string query = @"
SELECT 
    TRIGGER_NAME,
    TRIGGERING_EVENT AS type_desc
FROM 
    ALL_TRIGGERS
WHERE 
    OWNER = :schemaName
    AND TABLE_NAME = :tableName
    AND STATUS = 'ENABLED'
    AND TRIGGERING_EVENT IN ('INSERT', 'UPDATE')";

            return query;
        }

        public string QuoteTableNameAsDBConnectionParam(string param)
        {
            // Oracle uses same quoting for table names in parameters
            return QuoteIdentifier(param);
        }

        /// <summary>
        /// Builds select columns, handling special Oracle types like BLOB for base64 encoding.
        /// </summary>
        private string MakeSelectColumns(SqlQueryStructure structure)
        {
            List<string> builtColumns = new();

            foreach (LabelledColumn column in structure.Columns)
            {
                // Handle BLOB columns - convert to base64
                if (column.ColumnName != SqlQueryStructure.DATA_IDENT &&
                    structure.GetColumnSystemType(column.ColumnName) == typeof(byte[]))
                {
                    // Oracle: Convert BLOB to Base64 using UTL_ENCODE
                    builtColumns.Add($"UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_ENCODE({Build(column as Column)})) AS {QuoteIdentifier(column.Label)}");
                }
                else
                {
                    builtColumns.Add(Build(column));
                }
            }

            return string.Join(", ", builtColumns);
        }
    }
}
