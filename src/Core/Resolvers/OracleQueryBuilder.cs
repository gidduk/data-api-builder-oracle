// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Data;
using System.Data.Common;
using System.Text;
using System.Text.RegularExpressions;
using Azure.DataApiBuilder.Config.DatabasePrimitives;
using Azure.DataApiBuilder.Config.ObjectModel;
using Azure.DataApiBuilder.Core.Models;
using Oracle.ManagedDataAccess.Client;

namespace Azure.DataApiBuilder.Core.Resolvers
{
    /// <summary>
    /// Class for building Oracle queries.
    /// </summary>
    public class OracleQueryBuilder : BaseTSqlQueryBuilder, IQueryBuilder
    {
        private const string ORACLE_ESCAPE_CHAR = "\\";

        // Name of the column which stores the number of records with given PK. Used in Upsert queries.
        public const string COUNT_ROWS_WITH_GIVEN_PK = "cnt_rows_to_update";

        private static DbCommandBuilder _builder = new OracleCommandBuilder();

        private static readonly Dictionary<DbType, string> _typeMap = new()
        {
            { DbType.AnsiString, "VARCHAR2" },
            { DbType.StringFixedLength, "CHAR" },
            { DbType.Currency, "NUMBER" },
            { DbType.DateTime, "TIMESTAMP" },
            { DbType.Guid, "RAW(16)" },
            { DbType.Boolean, "Int16" },
            { DbType.Date, "DATE" },
            { DbType.Decimal, "NUMBER" },
            { DbType.Double, "NUMBER" },
            { DbType.Xml, "XmlType" },
            { DbType.String, "VARCHAR2" },
            { DbType.Binary, "BLOB" }
        };

        /// <inheritdoc />
        public override string QuoteIdentifier(string ident)
        {
            return _builder.QuoteIdentifier(ident);
        }

        /// <inheritdoc />
        public string Build(SqlQueryStructure structure)
        {
            string fromSql = $"{QuoteIdentifier(structure.DatabaseObject.Name)} {QuoteIdentifier(structure.SourceAlias)}{BuildJoin(structure.Joins)}";
            fromSql += string.Join("", structure.JoinQueries.Select(x => $" LEFT OUTER JOIN LATERAL ({Build(x.Value)}) {QuoteIdentifier(x.Key)} ON (1=1)"));

            string predicates = JoinPredicateStrings(
                                    structure.GetDbPolicyForOperation(EntityActionOperation.Read),
                                    structure.FilterPredicates,
                                    Build(structure.Predicates),
                                    Build(structure.PaginationMetadata.PaginationPredicate));

            string query = $"SELECT {Build(structure.Columns)}"
                + $" FROM {fromSql}"
                + $" WHERE {predicates}"
                + $" ORDER BY {Build(structure.OrderByColumns)}"
                + $" FETCH FIRST {structure.Limit()} ROWS ONLY";

            string subqueryName = QuoteIdentifier($"subq{structure.Counter.Next()}".ToUpper());

            StringBuilder result = new();
            if (structure.IsListQuery)
            {
                result.Append($"SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT({MakeJsonObjectParams(structure, subqueryName)}) RETURNING CLOB), TO_CLOB('[]')) ");
            }
            else
            {
                result.Append($"SELECT JSON_OBJECT({MakeJsonObjectParams(structure, subqueryName)}) ");
            }

            result.Append($"AS {QuoteIdentifier(SqlQueryStructure.DATA_IDENT)} FROM ( ");
            result.Append(query);
            result.Append($" ) {subqueryName}");

            return result.ToString();
        }

        /// <summary>
        /// Build and join each join with an empty separator
        /// </summary>
        protected string BuildJoin(List<SqlJoinStructure> joins)
        {
            return string.Join("", joins.Select(j => BuildQuery(j)));
        }

        /// <summary>
        /// Write the join in sql
        /// INNER JOIN {TableName} AS {SourceAlias} ON {JoinPredicates}
        /// </summary>
        protected string BuildQuery(SqlJoinStructure join)
        {
            if (join is null)
            {
                throw new ArgumentNullException(nameof(join));
            }

            if (!string.IsNullOrWhiteSpace(join.DbObject.SchemaName))
            {
                return $" INNER JOIN {QuoteIdentifier(join.DbObject.SchemaName)}.{QuoteIdentifier(join.DbObject.Name)} " +
                       $"{QuoteIdentifier(join.TableAlias)} " +
                       $"ON {Build(join.Predicates)}";
            }
            else
            {
                return $" INNER JOIN {QuoteIdentifier(join.DbObject.Name)} " +
                       $"{QuoteIdentifier(join.TableAlias)} " +
                       $"ON {Build(join.Predicates)}";
            }
        }

        /// <summary>
        /// Helper method to add ESCAPE clause to the LIKE clauses in the query.
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        private static string AddEscapeToLikeClauses(string predicate)
        {
            const string escapeClause = $" ESCAPE '{ORACLE_ESCAPE_CHAR}'";
            // Regex to find LIKE clauses and append ESCAPE
            return Regex.Replace(predicate, @"(LIKE\s+@[\w\d]+)", $"$1{escapeClause}", RegexOptions.IgnoreCase);
        }

        /// <inheritdoc />
        public string Build(SqlInsertStructure structure)
        {
            Dictionary<string, ColumnDefinition> columns = structure.DatabaseObject.SourceDefinition.Columns.Where(c => structure.InsertColumns.Any(i => i == c.Key)).ToDictionary();

            return $"DECLARE {Environment.NewLine} {string.Join($"", columns.Select(c => AddDeclareSection(c)))} {Environment.NewLine} " +
                $"BEGIN {Environment.NewLine} INSERT INTO {QuoteIdentifier(structure.DatabaseObject.SchemaName)}.{QuoteIdentifier(structure.DatabaseObject.Name)} ({Build(structure.InsertColumns)}) " +
            $"VALUES ({string.Join(", ", (structure.Values))})" +
                    $" RETURNING {Build(structure.InsertColumns)} INTO {string.Join(", ", structure.InsertColumns.Select(c => $"{QuoteIdentifier("OUT_" + c)}"))}; {Environment.NewLine}" +
                    $" OPEN :refcursor for SELECT {Build(structure.InsertColumns)} FROM {QuoteIdentifier(structure.DatabaseObject.SchemaName)}.{QuoteIdentifier(structure.DatabaseObject.Name)} WHERE {BuildInsertWhereClause(structure.InsertColumns)}; {Environment.NewLine} END;";
            ;
        }

        /// <summary>
        /// Quote and join list of strings with a ", " separator
        /// </summary>
        protected string BuildInsertWhereClause(List<string> columns)
        {
            return string.Join(" AND ", columns.Select(c => $"{QuoteIdentifier(c)} = {QuoteIdentifier("OUT_" + c)}"));
        }

        /// <inheritdoc />
        public string Build(SqlUpdateStructure structure)
        {
            SourceDefinition sourceDefinition = structure.GetUnderlyingSourceDefinition();
            bool isUpdateTriggerEnabled = sourceDefinition.IsUpdateDMLTriggerEnabled;
            string tableName = $"{QuoteIdentifier(structure.DatabaseObject.SchemaName)}.{QuoteIdentifier(structure.DatabaseObject.Name)}";
            string predicates = JoinPredicateStrings(
                                   structure.GetDbPolicyForOperation(EntityActionOperation.Update),
                                   Build(structure.Predicates));
            string columnsToBeReturned =
                MakeOutputColumns(structure.OutputColumns, isUpdateTriggerEnabled ? string.Empty : OutputQualifier.Inserted.ToString());

            StringBuilder updateQuery = new($"UPDATE {tableName} SET {Build(structure.UpdateOperations, ", ")} ");

            // If a trigger is enabled on the entity, we cannot use OUTPUT clause to return the record.
            // In such a case, we will use a subsequent select query to get the record. By the time the subsequent select executes,
            // the trigger would have already executed and we get the data as it is present in the table.
            if (isUpdateTriggerEnabled)
            {
                updateQuery.Append($"WHERE {predicates};");
                updateQuery.Append($"SELECT {columnsToBeReturned} FROM {tableName} WHERE {predicates};");
            }
            else
            {
                updateQuery.Append($"OUTPUT {columnsToBeReturned} WHERE {predicates};");
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
                    $"WHERE {predicates} ";
        }

        /// <inheritdoc />
        public string Build(SqlExecuteStructure structure)
        {
            return $"EXECUTE {QuoteIdentifier(structure.DatabaseObject.SchemaName)}.{QuoteIdentifier(structure.DatabaseObject.Name)} " +
                $"{BuildProcedureParameterList(structure.ProcedureParameters)}";
        }

        /// <inheritdoc />
        public string Build(SqlUpsertQueryStructure structure)
        {
            (string sets, string updates, string select) = MakeQuerySegmentsForUpdate(structure, structure.OutputColumns);

            if (structure.IsFallbackToUpdate)
            {
                return sets + ";\n" +
                    $"UPDATE {QuoteIdentifier(structure.DatabaseObject.Name)} " +
                    $"SET {Build(structure.UpdateOperations, ", ")} " +
                        ", " + updates +
                    $" WHERE {Build(structure.Predicates)}; " +
                    $" SET ROWCOUNT=ROW_COUNT(); " +
                    $"SELECT " + select + $" WHERE ROWCOUNT > 0;";
            }
            else
            {
                string insert = $"INSERT INTO {QuoteIdentifier(structure.DatabaseObject.Name)} ({Build(structure.InsertColumns)}) " +
                        $"VALUES ({string.Join(", ", (structure.Values))}) ";

                return sets + ";\n" +
                        insert + " ON DUPLICATE KEY " +
                        $"UPDATE {Build(structure.UpdateOperations, ", ")}" +
                        $", " + updates + ";" +
                        $" SET ROWCOUNT=ROW_COUNT(); " +
                        $"SELECT " + select + $" WHERE ROWCOUNT != 1;" +
                        $"SELECT {MakeUpsertSelections(structure)} WHERE ROWCOUNT = 1;";
            }
        }

        private string MakeUpsertSelections(SqlUpsertQueryStructure structure)
        {
            List<string> selections = new();
            Dictionary<string, string> insertColumnsToParamName = structure.InsertColumns.Zip(structure.Values, (colName, paramName)
                => new { Key = colName, Value = paramName }).ToDictionary(kv => kv.Key, kv => kv.Value);

            List<LabelledColumn> fields = structure.OutputColumns;

            foreach (LabelledColumn column in fields)
            {
                string quotedColName = QuoteIdentifier(column.Label);
                ColumnDefinition columnDefinition = structure.GetColumnDefinition(column.ColumnName);
                if (columnDefinition.IsAutoGenerated)
                {
                    selections.Add($"LAST_INSERT_ID() AS {quotedColName}");
                }
                else if (columnDefinition.IsReadOnly)
                {
                    // We cannot update a read-only column and hence cannot include it in the response.
                    continue;
                }
                else if (insertColumnsToParamName.TryGetValue(column.ColumnName, out string? paramName))
                {
                    selections.Add($"{paramName} AS {quotedColName}");
                }
                else if (columnDefinition.HasDefault)
                {
                    selections.Add($"{GetOracleDefaultValue(columnDefinition)} AS {quotedColName}");
                }
                else
                {
                    selections.Add($"NULL AS {quotedColName}");
                }
            }

            return string.Join(", ", selections);
        }

        /// <summary>
        /// Makes the query segments to store PK during an update. For each of the constructed segments, we do not include fields which are
        /// read-only because read-only fields cannot be included in an update statement as their value cannot be updated. And consequently,
        /// they cannot be included in the subsequent select statement as well.
        /// </summary>
        /// <param name="structure">Query structure of the update/upsert query.</param>
        /// <param name="outputColumns">List of columns to be returned.</param>
        /// <returns>A tuple of 3 strings where:
        /// 1. The first string is for the set clause: to create local variables to store the updatable columns.
        /// 2. The second string is for the update clause: to fetch the values of the updatable columns to local variables.
        /// 3. The third string is for the select clause: to select local variables and mapping to original column name.
        /// </returns>
        private (string, string, string) MakeQuerySegmentsForUpdate(BaseSqlQueryStructure structure, List<LabelledColumn> outputColumns)
        {
            SourceDefinition sourceDefinition = structure.GetUnderlyingSourceDefinition();
            List<string> columns = structure.AllColumns();

            // Create local variables to store the updatable columns.
            string sets = String.Join(";\n",
                columns.Where(col => !sourceDefinition.Columns[col].IsReadOnly || sourceDefinition.Columns[col].IsAutoGenerated)
                .Select((col, index) => $"SET {":LU_" + index.ToString()} := 0"));

            // Fetch the values of the updatable columns to local variables.
            string updates = String.Join(", ",
                columns.Where(col => !sourceDefinition.Columns[col].IsReadOnly || sourceDefinition.Columns[col].IsAutoGenerated)
                .Select((col, index) => $"{QuoteIdentifier(col)} = (SELECT {":LU_" + index.ToString()} := {QuoteIdentifier(col)})"));

            // Select local variables and mapping to original column name.
            string select = String.Join(", ",
                outputColumns.Where(col => !sourceDefinition.Columns[col.ColumnName].IsReadOnly || sourceDefinition.Columns[col.ColumnName].IsAutoGenerated)
                .Select((col, index) => $"{":LU_" + index.ToString()} AS {QuoteIdentifier(col.Label)}"));
            /*
             * An example tuple of sets,updates, and select would look like:
             * sets:
             * SET :LU_0 := 0
             * SET :LU_1 := 0;
             * SET :LU_2 := 0
             * updates:
             * `param0` = (SELECT :LU_0 := `param0`), `param1` = (SELECT :LU_1 := `param1`), `param2` = (SELECT :LU_2 := `param2`)
             * select:
             * :LU_0 AS `param0`, :LU_1 AS `param1`, :LU_2 AS `param2`
             */

            return (sets, updates, select);
        }

        /// <summary>
        /// Labels with which columns can be marked in the OUTPUT clause
        /// </summary>
        private enum OutputQualifier { Inserted, Deleted };

        /// <summary>
        /// Adds qualifiers (inserted or deleted) to output columns in OUTPUT clause
        /// and joins them with commas. e.g. for outputcolumns [C1, C2, C3] and output
        /// qualifier Inserted return
        /// Inserted.ColumnName1 AS {Label1}, Inserted.ColumnName2 AS {Label2},
        /// Inserted.ColumnName3 AS {Label3}
        /// </summary>
        private string MakeOutputColumns(List<LabelledColumn> columns, string columnPrefix)
        {
            return string.Join(", ", columns.Select(c => Build(c, columnPrefix)));
        }

        /// <summary>
        /// Build a labelled column as a column and attach
        /// ... AS {Label} to it
        /// </summary>
        private string Build(LabelledColumn column, string columnPrefix)
        {
            if (string.IsNullOrEmpty(columnPrefix))
            {
                return $"{QuoteIdentifier(column.ColumnName)} AS {QuoteIdentifier(column.Label)}";
            }

            return $"{columnPrefix}.{QuoteIdentifier(column.ColumnName)} AS {QuoteIdentifier(column.Label)}";
        }

        /// <summary>
        /// Builds the parameter list for the stored procedure execute call
        /// paramKeys are the user-generated procedure parameter names
        /// paramValues are the auto-generated, parameterized values (@param0, @param1..)
        /// </summary>
        private static string BuildProcedureParameterList(Dictionary<string, object> procedureParameters)
        {
            StringBuilder sb = new();
            foreach ((string paramKey, object paramValue) in procedureParameters)
            {
                sb.Append($":{paramKey} = {paramValue}, ");
            }

            string parameterList = sb.ToString();
            // If at least one parameter added, remove trailing comma and space, else return empty string
            return parameterList.Length > 0 ? parameterList[..^2] : parameterList;
        }

        /// <summary>
        /// Builds the query to fetch result set details of stored-procedure.
        /// result_field_name is the name of the result column.
        /// result_type contains the sql type, i.e char,int,varchar. Using TYPE_NAME method
        /// allows us to get the type without size constraints. example: TYPE_NAME for both
        /// varchar(100) and varchar(max) would be varchar.
        /// is_nullable is a boolean value to know if the result column is nullable or not.
        /// </summary>
        public string BuildStoredProcedureResultDetailsQuery(string databaseObjectName)
        {
            // The system type name column is aliased while the other columns are not to ensure
            // names are consistent across different sql implementations as all go through same deserialization logic
            string query = "SELECT " +
                            $"{STOREDPROC_COLUMN_NAME}, TYPE_NAME(system_type_id) as {STOREDPROC_COLUMN_SYSTEMTYPENAME}, {STOREDPROC_COLUMN_ISNULLABLE} " +
                            "FROM " +
                            "sys.dm_exec_describe_first_result_set_for_object (" +
                            $"OBJECT_ID('{databaseObjectName}'), 0) " +
                            "WHERE is_hidden is not NULL AND is_hidden = 0";
            return query;
        }

        /// <summary>
        /// Builds the query to get all the read-only columns in an MsSql table.
        /// For MsSql, the columns:
        /// 1. That have data_type of 'timestamp', or
        /// 2. are computed based on other columns,
        /// are considered as read only columns. The query combines both the types of read-only columns and returns the list.
        /// </summary>
        /// <param name="schemaOrDatabaseParamName">Param name of the schema/database.</param>
        /// <param name="tableParamName">Param name of the table.</param>
        /// <returns></returns>
        public string BuildQueryToGetReadOnlyColumns(string schemaParamName, string tableParamName)
        {
            string query = $"SELECT COLUMN_NAME FROM ALL_TAB_COLS WHERE OWNER = {schemaParamName} AND TABLE_NAME = {tableParamName} " +
               "AND VIRTUAL_COLUMN = 'YES' AND USER_GENERATED = 'YES' ORDER BY COLUMN_NAME";

            return query;
        }

        /// <inheritdoc/>
        public string BuildFetchEnabledTriggersQuery()
        {
            string query = "SELECT TRIGGERING_EVENT FROM ALL_TRIGGERS WHERE OWNER = :param0 AND TABLE_NAME = :param1 AND STATUS = 'ENABLED'";

            return query;
        }

        public string QuoteTableNameAsDBConnectionParam(string param)
        {
            // Table names in MSSQL should not be quoted when used as DB Connection Params.
            return param;
        }

        /// <inheritdoc />
        public override string BuildForeignKeyInfoQuery(int numberOfParameters)
        {
            string[] schemaNameParams =
                CreateParams(kindOfParam: SCHEMA_NAME_PARAM, numberOfParameters);

            string[] tableNameParams =
                CreateParams(kindOfParam: TABLE_NAME_PARAM, numberOfParameters);

            string tableSchemaParamsForInClause = string.Join(", ", schemaNameParams.Select(s => $":{s}"));
            string tableNameParamsForInClause = string.Join(", ", tableNameParams.Select(s => $":{s}"));

            // For Oracle, the foreign key query is constructed to fetch the foreign key details from user_constraints and user_cons_columns tables.
            // The schema is restricted to the current user (user_constraints) and does not require additional parameters.
            string foreignKeyQuery = $@"
                SELECT 
                    fk.constraint_name AS ""ForeignKeyDefinition"",
                    fk.owner AS ""ReferencingSchemaName"",
                    fk.table_name AS ""ReferencingSourceDefinition"",
                    fkc.column_name AS ""ReferencingColumns"",
                    pk.table_name AS ""ReferencedSourceDefinition"",
                    pk.owner AS ""ReferencedSchemaName"",
                    pkc.column_name AS ""ReferencedColumns""
                FROM user_constraints fk
                JOIN user_cons_columns fkc ON fk.constraint_name = fkc.constraint_name
                JOIN user_constraints pk ON fk.r_constraint_name = pk.constraint_name
                JOIN user_cons_columns pkc ON pk.constraint_name = pkc.constraint_name
                                           AND fkc.position = pkc.position
                WHERE fk.constraint_type = 'R' AND
                      fk.owner IN ({tableSchemaParamsForInClause}) AND
                      fk.table_name IN ({tableNameParamsForInClause})
                ORDER BY fk.table_name, fk.constraint_name, fkc.position";

            return foreignKeyQuery;
        }

        protected override string BuildPredicates(SqlQueryStructure structure)
        {
            string predicates;
            if (structure.IsMultipleCreateOperation)
            {
                predicates = JoinPredicateStrings(
                                    structure.GetDbPolicyForOperation(EntityActionOperation.Read),
                                    structure.FilterPredicates,
                                    Build(structure.Predicates, " OR ", isMultipleCreateOperation: true),
                                    Build(structure.PaginationMetadata.PaginationPredicate));
            }
            else
            {
                predicates = JoinPredicateStrings(
                                    structure.GetDbPolicyForOperation(EntityActionOperation.Read),
                                    structure.FilterPredicates,
                                    Build(structure.Predicates),
                                    Build(structure.PaginationMetadata.PaginationPredicate));
            }

            // we add '\' character to escape the special characters in the string, but if special characters are needed to be searched
            // as literal characters we need to escape the '\' character itself. Since we add `\` only for LIKE, so we search if the query
            // contains LIKE and add the ESCAPE clause accordingly.
            return AddEscapeToLikeClauses(predicates);
        }
        private static string GetOracleDefaultValue(ColumnDefinition column)
        {
            return column.DefaultValue!.ToString()!;
        }

        /// <summary>
        /// Makes the parameters for the JSON_OBJECT function from a list of labelled columns
        /// Format for table columns is:
        ///     "label1", subqueryName.label1, "label2", subqueryName.label2
        /// Format for subquery columns is:
        ///     "label1", JSON_EXTRACT(subqueryName.label1, '$'), "label2", JSON_EXTRACT(subqueryName.label2, '$')
        /// </summary>
        private string MakeJsonObjectParams(SqlQueryStructure structure, string subqueryName)
        {
            List<string> jsonColumns = new();
            foreach (LabelledColumn column in structure.Columns)
            {
                string cLabel = column.Label;
                string parametrizedCLabel = structure.ColumnLabelToParam[cLabel];

                // columns which contain the json of a nested type are called SqlQueryStructure.DATA_IDENT
                // and they are not actual columns of the underlying table so don't check for column type
                // in that scenario
                if (column.ColumnName != SqlQueryStructure.DATA_IDENT &&
                    structure.GetColumnSystemType(column.ColumnName) == typeof(byte[]))
                {
                    jsonColumns.Add($"{parametrizedCLabel}, TO_BASE64({subqueryName}.{QuoteIdentifier(cLabel)})");
                }
                else
                {
                    jsonColumns.Add($"'{cLabel}' : {subqueryName}.{QuoteIdentifier(cLabel)}");
                }
            }

            return string.Join(", ", jsonColumns);
        }

        /// <summary>
        /// Builds the declare statement
        /// </summary>
        /// <param name="declareColumn">colums which will be added to the declare section, not all output columns will be here.</param>
        /// <returns>column name with oracle datatype</returns>
        /// <exception cref="ArgumentException">if not matching oracle type is found</exception>
        private string? AddDeclareSection(KeyValuePair<string, ColumnDefinition> declareColumn)
        {
            if (_typeMap.TryGetValue(declareColumn.Value.DbType!.Value, out string? oracleDbType))
            {
                return $"{QuoteIdentifier($"OUT_{declareColumn.Key}")} {oracleDbType}{((oracleDbType == "VARCHAR2") ? $"({(declareColumn.Value).MaxLength})" : string.Empty)}; {Environment.NewLine}";
            }

            throw new ArgumentException($"No Oracle type mapping exists for {declareColumn.GetType().Name}");
        }
    }
}
