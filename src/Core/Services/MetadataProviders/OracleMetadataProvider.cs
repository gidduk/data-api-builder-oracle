// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Data;
using System.Net;
using Azure.DataApiBuilder.Config.DatabasePrimitives;
using Azure.DataApiBuilder.Config.ObjectModel;
using Azure.DataApiBuilder.Core.Configurations;
using Azure.DataApiBuilder.Core.Resolvers.Factories;
using Azure.DataApiBuilder.Service.Exceptions;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;

namespace Azure.DataApiBuilder.Core.Services
{
    /// <summary>
    /// Oracle 23ai specific metadata provider supporting tables, views, stored procedures, JSON and Vector columns.
    /// </summary>
    public class OracleMetadataProvider : SqlMetadataProvider<OracleConnection, OracleDataAdapter, OracleCommand>
    {
        private readonly RuntimeConfigProvider _runtimeConfigProvider;

        public OracleMetadataProvider(
            RuntimeConfigProvider runtimeConfigProvider,
            IAbstractQueryManagerFactory queryManagerFactory,
            ILogger<ISqlMetadataProvider> logger,
            string dataSourceName,
            bool isValidateOnly = false)
            : base(runtimeConfigProvider, queryManagerFactory, logger, dataSourceName, isValidateOnly)
        {
            _runtimeConfigProvider = runtimeConfigProvider;
        }

        /// <summary>
        /// Returns the default schema name for Oracle (typically the username/schema).
        /// </summary>
        public override string GetDefaultSchemaName()
        {
            // Oracle doesn't have a universal default schema like SQL Server's "dbo"
            // The schema is typically the username. We'll need to extract it from connection string
            // or query the database
            OracleConnectionStringBuilder builder = new(ConnectionString);
            return builder.UserID.ToUpperInvariant();
        }

        /// <summary>
        /// Maps Oracle data types to .NET CLR types.
        /// Supports Oracle 23ai types including JSON and VECTOR.
        /// </summary>
        public override Type SqlToCLRType(string oracleTypeName)
        {
            // Remove precision/scale specifications
            int parenIndex = oracleTypeName.IndexOf('(');
            string baseType = parenIndex == -1 ? oracleTypeName : oracleTypeName.Substring(0, parenIndex);
            baseType = baseType.ToUpperInvariant().Trim();

            return baseType switch
            {
                // Numeric types
                "NUMBER" => typeof(decimal),
                "FLOAT" => typeof(double),
                "BINARY_FLOAT" => typeof(float),
                "BINARY_DOUBLE" => typeof(double),
                "INTEGER" => typeof(int),
                "INT" => typeof(int),
                "SMALLINT" => typeof(short),
                
                // String types
                "VARCHAR2" => typeof(string),
                "NVARCHAR2" => typeof(string),
                "CHAR" => typeof(string),
                "NCHAR" => typeof(string),
                "CLOB" => typeof(string),
                "NCLOB" => typeof(string),
                "LONG" => typeof(string),
                
                // Date/Time types
                "DATE" => typeof(DateTime),
                "TIMESTAMP" => typeof(DateTime),
                "TIMESTAMP WITH TIME ZONE" => typeof(DateTimeOffset),
                "TIMESTAMP WITH LOCAL TIME ZONE" => typeof(DateTimeOffset),
                "INTERVAL YEAR TO MONTH" => typeof(TimeSpan),
                "INTERVAL DAY TO SECOND" => typeof(TimeSpan),
                
                // Binary types
                "RAW" => typeof(byte[]),
                "BLOB" => typeof(byte[]),
                "BFILE" => typeof(byte[]),
                "LONG RAW" => typeof(byte[]),
                
                // Oracle 23ai specific types
                "JSON" => typeof(string),  // JSON stored as string, parsed as needed
                "VECTOR" => typeof(string), // Vector stored as string representation
                
                // Other types
                "ROWID" => typeof(string),
                "UROWID" => typeof(string),
                "XMLTYPE" => typeof(string),
                "BOOLEAN" => typeof(bool),
                
                _ => typeof(string) // Default to string for unknown types
            };
        }

        /// <summary>
        /// Maps Oracle data type names to OracleDbType enum values.
        /// </summary>
        private static OracleDbType? GetOracleDbType(string oracleTypeName)
        {
            // Remove precision/scale specifications
            int parenIndex = oracleTypeName.IndexOf('(');
            string baseType = parenIndex == -1 ? oracleTypeName : oracleTypeName.Substring(0, parenIndex);
            baseType = baseType.ToUpperInvariant().Trim();

            return baseType switch
            {
                "NUMBER" => OracleDbType.Decimal,
                "FLOAT" => OracleDbType.Double,
                "BINARY_FLOAT" => OracleDbType.BinaryFloat,
                "BINARY_DOUBLE" => OracleDbType.BinaryDouble,
                "INTEGER" => OracleDbType.Int32,
                "INT" => OracleDbType.Int32,
                "SMALLINT" => OracleDbType.Int16,
                
                "VARCHAR2" => OracleDbType.Varchar2,
                "NVARCHAR2" => OracleDbType.NVarchar2,
                "CHAR" => OracleDbType.Char,
                "NCHAR" => OracleDbType.NChar,
                "CLOB" => OracleDbType.Clob,
                "NCLOB" => OracleDbType.NClob,
                "LONG" => OracleDbType.Long,
                
                "DATE" => OracleDbType.Date,
                "TIMESTAMP" => OracleDbType.TimeStamp,
                "TIMESTAMP WITH TIME ZONE" => OracleDbType.TimeStampTZ,
                "TIMESTAMP WITH LOCAL TIME ZONE" => OracleDbType.TimeStampLTZ,
                "INTERVAL YEAR TO MONTH" => OracleDbType.IntervalYM,
                "INTERVAL DAY TO SECOND" => OracleDbType.IntervalDS,
                
                "RAW" => OracleDbType.Raw,
                "BLOB" => OracleDbType.Blob,
                "BFILE" => OracleDbType.BFile,
                "LONG RAW" => OracleDbType.LongRaw,
                
                "JSON" => OracleDbType.Clob, // Oracle 23ai JSON stored as CLOB
                "VECTOR" => OracleDbType.Clob, // Oracle 23ai VECTOR stored as CLOB
                
                "ROWID" => OracleDbType.Varchar2,
                "UROWID" => OracleDbType.Varchar2,
                "XMLTYPE" => OracleDbType.XmlType,
                "BOOLEAN" => OracleDbType.Boolean,
                
                _ => null
            };
        }

        /// <summary>
        /// Gets column metadata from Oracle data dictionary.
        /// </summary>
        protected override async Task<DataTable> GetColumnsAsync(string schemaName, string tableName)
        {
            using OracleConnection conn = new();
            conn.ConnectionString = ConnectionString;
            await QueryExecutor.SetManagedIdentityAccessTokenIfAnyAsync(conn, _dataSourceName);
            await conn.OpenAsync();

            DataTable columnsTable = new();
            columnsTable.Columns.Add("COLUMN_NAME", typeof(string));
            columnsTable.Columns.Add("DATA_TYPE", typeof(string));
            columnsTable.Columns.Add("COLUMN_DEFAULT", typeof(object));
            columnsTable.Columns.Add("IS_NULLABLE", typeof(string));
            columnsTable.Columns.Add("DATA_LENGTH", typeof(int));
            columnsTable.Columns.Add("DATA_PRECISION", typeof(int));
            columnsTable.Columns.Add("DATA_SCALE", typeof(int));
            columnsTable.Columns.Add("VIRTUAL_COLUMN", typeof(bool));
            columnsTable.Columns.Add("IDENTITY_COLUMN", typeof(bool));
            string query = @"
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                DATA_DEFAULT AS COLUMN_DEFAULT,
                NULLABLE AS IS_NULLABLE,
                DATA_LENGTH,
                DATA_PRECISION,
                DATA_SCALE,
                VIRTUAL_COLUMN,
                IDENTITY_COLUMN
            FROM 
                ALL_TAB_COLS
            WHERE 
                OWNER = :schemaName 
                AND TABLE_NAME = :tableName
            ORDER BY 
                COLUMN_ID";

            using (OracleCommand cmd = new(query, conn))
            {
                cmd.InitialLONGFetchSize = 4000;
                cmd.Parameters.Add(new OracleParameter("schemaName", schemaName.ToUpperInvariant()));
                cmd.Parameters.Add(new OracleParameter("tableName", tableName.ToUpperInvariant()));

                using (OracleDataReader reader = (OracleDataReader)await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        DataRow row = columnsTable.NewRow();
                        row["COLUMN_NAME"] = reader["COLUMN_NAME"];
                        row["DATA_TYPE"] = reader["DATA_TYPE"];
                        row["COLUMN_DEFAULT"] = reader.IsDBNull(reader.GetOrdinal("COLUMN_DEFAULT")) 
                            ? DBNull.Value 
                            : reader["COLUMN_DEFAULT"];
                        row["IS_NULLABLE"] = reader["IS_NULLABLE"];
                        row["DATA_LENGTH"] = reader.IsDBNull(reader.GetOrdinal("DATA_LENGTH")) 
                            ? DBNull.Value 
                            : reader["DATA_LENGTH"];
                        row["DATA_PRECISION"] = reader.IsDBNull(reader.GetOrdinal("DATA_PRECISION")) 
                            ? DBNull.Value 
                            : reader["DATA_PRECISION"];
                        row["DATA_SCALE"] = reader.IsDBNull(reader.GetOrdinal("DATA_SCALE")) 
                            ? DBNull.Value 
                            : reader["DATA_SCALE"];
                        row["VIRTUAL_COLUMN"] = reader.IsDBNull(reader.GetOrdinal("VIRTUAL_COLUMN"))
                           ? false
                           : (reader["VIRTUAL_COLUMN"].ToString() == "YES");
                        row["IDENTITY_COLUMN"] = reader.IsDBNull(reader.GetOrdinal("IDENTITY_COLUMN"))
                           ? false
                           : (reader["IDENTITY_COLUMN"].ToString() == "YES");
                        columnsTable.Rows.Add(row);
                    }
                }
            }

            return columnsTable;
        }

        /// <summary>
        /// Query to identify read-only (virtual/generated) columns in Oracle.
        /// </summary>
        public static string BuildQueryToGetReadOnlyColumns(string schemaParamName, string tableParamName)
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

        /// <summary>
        /// Oracle-specific implementation that uses the comprehensive metadata from GetColumnsAsync
        /// to populate the SourceDefinition efficiently in a single database query.
        /// </summary>
        public override async Task PopulateSourceDefinitionAsync(
            string entityName,
            string schemaName,
            string tableName,
            SourceDefinition sourceDefinition,
            List<string> pkFields)
        {
            sourceDefinition.PrimaryKey = [.. pkFields];

            if (sourceDefinition.PrimaryKey.Count == 0)
            {
                throw new DataApiBuilderException(
                    message: $"Primary key not configured on the given database object {tableName}",
                    statusCode: HttpStatusCode.ServiceUnavailable,
                    subStatusCode: DataApiBuilderException.SubStatusCodes.ErrorInInitialization);
            }

            // Get comprehensive column metadata from Oracle data dictionary
            DataTable columnsInTable = await GetColumnsAsync(schemaName, tableName);

            RuntimeConfig runtimeConfig = _runtimeConfigProvider.GetConfig();
            _entities.TryGetValue(entityName, out Entity? entity);

            // Populate column definitions from Oracle metadata
            foreach (DataRow columnInfo in columnsInTable.Rows)
            {
                string columnName = (string)columnInfo["COLUMN_NAME"];

                // Check for GraphQL reserved names
                if (runtimeConfig.IsGraphQLEnabled
                    && entity is not null
                    && IsGraphQLReservedName(entity, columnName, graphQLEnabledGlobally: runtimeConfig.IsGraphQLEnabled))
                {
                    throw new DataApiBuilderException(
                        message: $"The column '{columnName}' violates GraphQL name restrictions.",
                        statusCode: HttpStatusCode.ServiceUnavailable,
                        subStatusCode: DataApiBuilderException.SubStatusCodes.ErrorInInitialization);
                }

                // Get data type and map to CLR type
                string oracleDataType = (string)columnInfo["DATA_TYPE"];
                Type systemType = SqlToCLRType(oracleDataType);

                // Oracle uses 'Y' or 'N' for IS_NULLABLE
                bool isNullable = columnInfo["IS_NULLABLE"].ToString() == "Y";

                // Check if column has a default value
                bool hasDefault = columnInfo["COLUMN_DEFAULT"] != DBNull.Value
                    && !string.IsNullOrWhiteSpace(columnInfo["COLUMN_DEFAULT"]?.ToString());

                bool isIdentityColumn = columnInfo["IDENTITY_COLUMN"].ToString() == "True";

                bool isReadOnly = columnInfo["VIRTUAL_COLUMN"].ToString() == "True";

                // Create column definition with all metadata
                ColumnDefinition column = new()
                {
                    SystemType = systemType,
                    IsNullable = isNullable,
                    HasDefault = hasDefault,
                    DefaultValue = hasDefault ? columnInfo["COLUMN_DEFAULT"] : null,
                    DbType = TypeHelper.GetDbTypeFromSystemType(systemType),
                    OracleDbType = GetOracleDbType(oracleDataType),
                    IsAutoGenerated = isIdentityColumn,
                    IsReadOnly = (entity is not null && entity.Source.Type is EntitySourceType.Table) 
                        ? isReadOnly : false
                };

                // Add column to source definition
                sourceDefinition.Columns.TryAdd(columnName, column);
            }
        }

        /// <summary>
        /// Builds query to get stored procedure result set metadata.
        /// </summary>
        public string BuildStoredProcedureResultDetailsQuery(string databaseObjectName)
        {
            // Oracle doesn't provide metadata for procedure result sets as easily as SQL Server
            // This would need to be implemented based on specific Oracle procedure metadata views
            throw new NotImplementedException(
                "Stored procedure result metadata querying for Oracle requires runtime execution analysis.");
        }
    }
}
