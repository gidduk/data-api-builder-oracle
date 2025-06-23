// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Data;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using Azure.DataApiBuilder.Config.DatabasePrimitives;
using Azure.DataApiBuilder.Core.Configurations;
using Azure.DataApiBuilder.Core.Models;
using Azure.DataApiBuilder.Core.Resolvers;
using Azure.DataApiBuilder.Core.Resolvers.Factories;
using Azure.DataApiBuilder.Service.Exceptions;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;

namespace Azure.DataApiBuilder.Core.Services.MetadataProviders
{
    public class OracleMetadataProvider : SqlMetadataProvider<OracleConnection, OracleDataAdapter, OracleCommand>
    {
        private readonly RuntimeConfigProvider _runtimeConfigProvider;

        public const string PARAM_NAME_PREFIX = ":";

        /// <summary>
        /// Query to get the primary key column names of a table from the Oracle metadata.
        /// </summary>
        const string PRIMARYKEYQUERY = @"SELECT column_name 
    FROM user_cons_columns 
    WHERE constraint_name = (
        SELECT constraint_name 
        FROM user_constraints 
        WHERE table_name = :tableName 
        AND constraint_type = 'P')";

        // Oracle supports only 3 restrictions
        // https://docs.oracle.com/en/database/oracle/oracle-database/23/odpnt/ConnectionGetSchema3.html#GUID-35E9B896-2953-4BE4-BB06-B0DE395DB367
        protected new const int NUMBER_OF_RESTRICTIONS = 3;

        public OracleMetadataProvider(RuntimeConfigProvider runtimeConfigProvider, IAbstractQueryManagerFactory queryManagerFactory, ILogger<ISqlMetadataProvider> logger, string dataSourceName, bool isValidateOnly = false) : base(runtimeConfigProvider, queryManagerFactory, logger, dataSourceName, isValidateOnly)
        {
            _runtimeConfigProvider = runtimeConfigProvider;
        }

        /// <summary>
        /// Takes a string version of an Oracle data type
        /// and returns its .NET common language runtime (CLR) counterpart
        /// As per https://docs.microsoft.com/dotnet/framework/data/adonet/sql-server-data-type-mappings
        /// </summary>
        public override Type SqlToCLRType(string sqlType)
        {
            _runtimeConfigProvider.GetConfig();
            return TypeHelper.OracleDbTypeToCLRType(sqlType);
        }

        /// <summary>
        /// Gets the metadata information of each column of
        /// the given schema.table
        /// </summary>
        /// <returns>A data table where each row corresponds to a
        /// column of the table.</returns>
        protected override async Task<DataTable> GetColumnsAsync(
            string schemaName,
            string tableName)
        {
            using OracleConnection conn = new();
            conn.ConnectionString = ConnectionString;
            await QueryExecutor.SetManagedIdentityAccessTokenIfAnyAsync(conn, _dataSourceName);
            await conn.OpenAsync();
            // We can specify the Connection, Schema, Table Name to get
            // the specified column(s).
            // Hence, we should create a 3 members array.
            string[] columnRestrictions =
            [
                // To restrict the columns for the current table, specify the table's name
                // in column restrictions.
                conn.Database,
                schemaName,
                tableName,
            ];

            // Each row in the columnsInTable DataTable corresponds to
            // a single column of the table.
            DataTable columnsInTable = await conn.GetSchemaAsync("Columns", columnRestrictions);

            return columnsInTable;
        }

        /// <inheritdoc/>
        public override async Task PopulateTriggerMetadataForTable(string entityName, string schemaName, string tableName, SourceDefinition sourceDefinition)
        {
            string enumerateEnabledTriggers = SqlQueryBuilder.BuildFetchEnabledTriggersQuery();
            Dictionary<string, DbConnectionParam> parameters = new()
            {
                { $"{PARAM_NAME_PREFIX}param0", new(schemaName, DbType.String) },
                { $"{PARAM_NAME_PREFIX}param1", new(tableName, DbType.String) }
            };

            JsonArray? resultArray = await QueryExecutor.ExecuteQueryAsync(
                sqltext: enumerateEnabledTriggers,
                parameters: parameters,
                dataReaderHandler: QueryExecutor.GetJsonArrayAsync,
                dataSourceName: _dataSourceName);
            using JsonDocument sqlResult = JsonDocument.Parse(resultArray!.ToJsonString());

            foreach (JsonElement element in sqlResult.RootElement.EnumerateArray())
            {
                string type_desc = element.GetProperty("type_desc").ToString();
                if ("UPDATE".Equals(type_desc))
                {
                    sourceDefinition.IsUpdateDMLTriggerEnabled = true;
                    _logger.LogInformation($"An update trigger is enabled for the entity: {entityName}");
                }

                if ("INSERT".Equals(type_desc))
                {
                    sourceDefinition.IsInsertDMLTriggerEnabled = true;
                    _logger.LogInformation($"An insert trigger is enabled for the entity: {entityName}");
                }
            }
        }

        /// <summary>
        /// Helper method to populate the column definitions of each column in a table with the info about
        /// whether the column can be updated or not.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="schemaOrDatabaseName">Name of the schema (for MsSql/PgSql)/database (for MySql) of the table.</param>
        /// <param name="sourceDefinition">Table definition.</param>
        public override async Task PopulateColumnDefinitionsWithReadOnlyFlag(string tableName, string schemaOrDatabaseName, SourceDefinition sourceDefinition)
        {
            string schemaOrDatabaseParamName = $"{PARAM_NAME_PREFIX}param0";
            string quotedTableName = SqlQueryBuilder.QuoteTableNameAsDBConnectionParam(tableName);
            string tableParamName = $"{PARAM_NAME_PREFIX}param1";
            string queryToGetReadOnlyColumns = SqlQueryBuilder.BuildQueryToGetReadOnlyColumns(schemaOrDatabaseParamName, tableParamName);
            Dictionary<string, DbConnectionParam> parameters = new()
            {
                { schemaOrDatabaseParamName, new(schemaOrDatabaseName, DbType.String) },
                { tableParamName, new(quotedTableName, DbType.String) }
            };

            List<string>? readOnlyFields = await QueryExecutor.ExecuteQueryAsync(
                sqltext: queryToGetReadOnlyColumns,
                parameters: parameters,
                dataReaderHandler: SummarizeReadOnlyFieldsMetadata,
                dataSourceName: _dataSourceName);

            if (readOnlyFields is not null && readOnlyFields.Count > 0)
            {
                foreach (string readOnlyField in readOnlyFields)
                {
                    if (sourceDefinition.Columns.TryGetValue(readOnlyField, out ColumnDefinition? columnDefinition))
                    {
                        // Mark the column as read-only.
                        columnDefinition.IsReadOnly = true;
                    }
                }
            }
        }

        /// <inheritdoc />
        public override string GetDefaultSchemaName()
        {
            // In Oracle, the default schema is the same as the username.
            OracleConnectionStringBuilder builder = new(ConnectionString);
            return builder.UserID ?? string.Empty;
        }

        /// <inheritdoc />
        public override async Task<DataTable> FillSchemaForTableAsync(
            string schemaName,
            string tableName)
        {
            using OracleConnection conn = new();
            // If connection string is set to empty string
            // we throw here to avoid having to sort out
            // complicated db specific exception messages.
            // This is caught and returned as DataApiBuilderException.
            // The runtime config has a public setter so we check
            // here for empty connection string to ensure that
            // it was not set to an invalid state after initialization.
            if (string.IsNullOrWhiteSpace(ConnectionString))
            {
                throw new DataApiBuilderException(
                    DataApiBuilderException.CONNECTION_STRING_ERROR_MESSAGE +
                    " Connection string is null, empty, or whitespace.",
                    statusCode: HttpStatusCode.ServiceUnavailable,
                    subStatusCode: DataApiBuilderException.SubStatusCodes.ErrorInInitialization);
            }

            try
            {
                // for non-MySql DB types, this will throw an exception
                // for malformed connection strings
                conn.ConnectionString = ConnectionString;
                await QueryExecutor.SetManagedIdentityAccessTokenIfAnyAsync(conn, _dataSourceName);
            }
            catch (Exception ex)
            {
                string message = DataApiBuilderException.CONNECTION_STRING_ERROR_MESSAGE +
                    $" Underlying Exception message: {ex.Message}";
                throw new DataApiBuilderException(
                    message,
                    statusCode: HttpStatusCode.ServiceUnavailable,
                    subStatusCode: DataApiBuilderException.SubStatusCodes.ErrorInInitialization,
                    innerException: ex);
            }

            await conn.OpenAsync();

            OracleDataAdapter adapterForTable = new();
            OracleCommand selectCommand = new()
            {
                Connection = conn
            };

            string tableNameWithSchemaPrefix = GetTableNameWithSchemaPrefix(schemaName, tableName);
            selectCommand.CommandText
                = $"SELECT * FROM {tableNameWithSchemaPrefix}";
            adapterForTable.SelectCommand = selectCommand;

            DataTable[] dataTable = adapterForTable.FillSchema(EntitiesDataSet, SchemaType.Source, tableNameWithSchemaPrefix);

            // If the FillSchema method does not return any Primary keys then get it from the contrainst.

            if (dataTable.Length != 0 && dataTable[0].PrimaryKey.Length == 0)
            {
                List<DataColumn> primaryKeyColumns = new();

                using OracleCommand command = new(PRIMARYKEYQUERY, conn);
                command.BindByName = true;
                command.Parameters.Add(":tableName", OracleDbType.Varchar2).Value = tableName;
                using OracleDataReader reader = await command.ExecuteReaderAsync();
                {
                    while (await reader.ReadAsync())
                    {
                        string? columnName = reader.GetString("column_name");
                        if (!string.IsNullOrEmpty(columnName) && dataTable[0].Columns.Contains(columnName))
                        {
                            DataColumn? column = dataTable[0].Columns[columnName];
                            if (column is not null)
                            {
                                primaryKeyColumns.Add(column);
                            }
                        }
                    }
                }

                dataTable[0].PrimaryKey = primaryKeyColumns.ToArray();
            }

            return dataTable[0];
        }
    }
}
