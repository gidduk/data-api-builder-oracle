// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Data;
using System.Data.Common;
using System.Net;
using Azure.Core;
using Azure.DataApiBuilder.Config;
using Azure.DataApiBuilder.Config.ObjectModel;
using Azure.DataApiBuilder.Core.Configurations;
using Azure.DataApiBuilder.Core.Models;
using Azure.DataApiBuilder.Service.Exceptions;
using Azure.Identity;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;

namespace Azure.DataApiBuilder.Core.Resolvers
{
    /// <summary>
    /// Specialized QueryExecutor for Oracle 23ai mainly providing methods to
    /// handle connecting to the database with a managed identity and Oracle-specific features.
    /// </summary>
    public class OracleQueryExecutor : QueryExecutor<OracleConnection>
    {
        // Azure scope for Oracle Database access via managed identity
        // This scope is used for Azure Database for Oracle services
        public const string DATABASE_SCOPE = @"https://ossrdbms-aad.database.windows.net/.default";

        /// <summary>
        /// The managed identity Access Token string obtained from the configuration controller.
        /// Key: datasource name, Value: access token for this datasource.
        /// </summary>
        private Dictionary<string, string?> _accessTokensFromConfiguration;

        /// <summary>
        /// The Oracle specific connection string builders.
        /// Key: datasource name, Value: connection string builder for this datasource.
        /// </summary>
        public override IDictionary<string, DbConnectionStringBuilder> ConnectionStringBuilders
            => base.ConnectionStringBuilders;

        public DefaultAzureCredential AzureCredential { get; set; } = new();  // CodeQL [SM05137] DefaultAzureCredential will use Managed Identity if available or fallback to default.

        /// <summary>
        /// The saved cached access token obtained from DefaultAzureCredentials
        /// representing a managed identity.
        /// </summary>
        private AccessToken? _defaultAccessToken;

        /// <summary>
        /// DatasourceName to boolean value indicating if access token should be set for db.
        /// </summary>
        private Dictionary<string, bool> _dataSourceAccessTokenUsage;

        private readonly RuntimeConfigProvider _runtimeConfigProvider;

        public OracleQueryExecutor(
            RuntimeConfigProvider runtimeConfigProvider,
            DbExceptionParser dbExceptionParser,
            ILogger<IQueryExecutor> logger,
            IHttpContextAccessor httpContextAccessor,
            HotReloadEventHandler<HotReloadEventArgs>? handler = null)
            : base(dbExceptionParser,
                  logger,
                  runtimeConfigProvider,
                  httpContextAccessor,
                  handler)
        {
            _dataSourceAccessTokenUsage = new Dictionary<string, bool>();
            _accessTokensFromConfiguration = runtimeConfigProvider.ManagedIdentityAccessToken;
            _runtimeConfigProvider = runtimeConfigProvider;
            ConfigureOracleQueryExecutor();
        }

        /// <summary>
        /// Creates an OracleConnection to the data source of given name.
        /// </summary>
        /// <param name="dataSourceName">The name of the data source.</param>
        /// <returns>The OracleConnection</returns>
        /// <exception cref="DataApiBuilderException">Exception thrown if datasource is not found.</exception>
        public override OracleConnection CreateConnection(string dataSourceName)
        {
            if (!ConnectionStringBuilders.ContainsKey(dataSourceName))
            {
                throw new DataApiBuilderException(
                    "Query execution failed. Could not find datasource to execute query against",
                    HttpStatusCode.BadRequest,
                    DataApiBuilderException.SubStatusCodes.DataSourceNotFound);
            }

            OracleConnection conn = new()
            {
                ConnectionString = ConnectionStringBuilders[dataSourceName].ConnectionString,
            };

            return conn;
        }

        /// <summary>
        /// Configure during construction or a hot-reload scenario.
        /// </summary>
        private void ConfigureOracleQueryExecutor()
        {
            IEnumerable<KeyValuePair<string, DataSource>> oracledbs = _runtimeConfigProvider.GetConfig()
                .GetDataSourceNamesToDataSourcesIterator()
                .Where(x => x.Value.DatabaseType is DatabaseType.Oracle);

            foreach ((string dataSourceName, DataSource dataSource) in oracledbs)
            {
                OracleConnectionStringBuilder builder = new(dataSource.ConnectionString);

                if (_runtimeConfigProvider.IsLateConfigured)
                {
                    // For Azure-hosted Oracle, ensure secure connections
                    // Oracle doesn't have the same Encrypt property as SQL Server
                    // Security is typically configured via wallet or SSL settings in TNS
                }

                ConnectionStringBuilders.TryAdd(dataSourceName, builder);
                _dataSourceAccessTokenUsage[dataSourceName] = ShouldManagedIdentityAccessBeAttempted(builder);
            }
        }

        /// <summary>
        /// Modifies the properties of the supplied connection to support managed identity access.
        /// In the case of Oracle, gets access token if deemed necessary and sets it as the password.
        /// The supplied connection is assumed to already have the same connection string
        /// provided in the runtime configuration.
        /// </summary>
        /// <param name="conn">The supplied connection to modify for managed identity access.</param>
        /// <param name="dataSourceName">Name of datasource for which to set access token. Default dbName taken from config if null</param>
        public override async Task SetManagedIdentityAccessTokenIfAnyAsync(DbConnection conn, string dataSourceName)
        {
            // using default datasource name for first db - maintaining backward compatibility for single db scenario.
            string effectiveDataSourceName = string.IsNullOrEmpty(dataSourceName)
                ? ConfigProvider.GetConfig().DefaultDataSourceName
                : dataSourceName;

            _dataSourceAccessTokenUsage.TryGetValue(effectiveDataSourceName, out bool setAccessToken);

            // Only attempt to get the access token if the connection string is in the appropriate format
            if (setAccessToken)
            {

                // If the configuration controller provided a managed identity access token use that,
                // else use the default saved access token if still valid.
                // Get a new token only if the saved token is null or expired.
                _accessTokensFromConfiguration.TryGetValue(effectiveDataSourceName, out string? accessTokenFromController);
                string? accessToken = accessTokenFromController ??
                    (IsDefaultAccessTokenValid() ?
                        ((AccessToken)_defaultAccessToken!).Token :
                        await GetAccessTokenAsync());

                if (accessToken is not null)
                {
                    // For Oracle, the access token is typically used as the password
                    OracleConnectionStringBuilder connstr = new(conn.ConnectionString)
                    {
                        Password = accessToken
                    };
                    conn.ConnectionString = connstr.ConnectionString;
                }
            }
        }

        /// <summary>
        /// Determines if managed identity access should be attempted or not.
        /// It should only be attempted if:
        /// 1. User ID is specified (required for Oracle)
        /// 2. Password is not specified (will be replaced with access token)
        /// </summary>
        private static bool ShouldManagedIdentityAccessBeAttempted(OracleConnectionStringBuilder builder)
        {
            return !string.IsNullOrEmpty(builder.UserID) &&
                   string.IsNullOrEmpty(builder.Password);
        }

        /// <summary>
        /// Determines if the saved default azure credential's access token is valid and not expired.
        /// </summary>
        /// <returns>True if valid, false otherwise.</returns>
        private bool IsDefaultAccessTokenValid()
        {
            return _defaultAccessToken is not null &&
                ((AccessToken)_defaultAccessToken).ExpiresOn.CompareTo(DateTimeOffset.Now) > 0;
        }

        /// <summary>
        /// Tries to get an access token using DefaultAzureCredentials.
        /// Catches any CredentialUnavailableException and logs only a warning
        /// since this is best effort.
        /// </summary>
        /// <returns>The string representation of the access token if found,
        /// null otherwise.</returns>
        private async Task<string?> GetAccessTokenAsync()
        {
            try
            {
                _defaultAccessToken = await AzureCredential.GetTokenAsync(
                    new TokenRequestContext(new[] { DATABASE_SCOPE }));
            }
            catch (CredentialUnavailableException ex)
            {
                string correlationId = HttpContextExtensions.GetLoggerCorrelationId(HttpContextAccessor.HttpContext);
                QueryExecutorLogger.LogWarning(
                    message: "{correlationId} Failed to retrieve a managed identity access token using DefaultAzureCredential due to:\n{errorMessage}",
                    correlationId,
                    ex.Message);
            }

            return _defaultAccessToken?.Token;
        }

        /// <summary>
        /// Method to generate the query to send user data to the underlying database via DBMS_SESSION
        /// which might be used for additional security at the database level.
        /// Oracle doesn't have SESSION_CONTEXT like SQL Server, but supports application context.
        /// </summary>
        /// <param name="httpContext">Current user httpContext.</param>
        /// <param name="parameters">Dictionary of parameters/value required to execute the query.</param>
        /// <param name="dataSourceName">Name of datasource for which to set session context.</param>
        /// <returns>empty string / query to set session parameters for the connection.</returns>
        public override string GetSessionParamsQuery(
            HttpContext? httpContext,
            IDictionary<string, DbConnectionParam> parameters,
            string dataSourceName)
        {
            //string effectiveDataSourceName = string.IsNullOrEmpty(dataSourceName)
            //    ? ConfigProvider.GetConfig().DefaultDataSourceName
            //    : dataSourceName;

            // For Oracle, we can use DBMS_SESSION.SET_CONTEXT or application context
            // This is a placeholder for future implementation of Oracle-specific session context
            // Currently returning empty string as Oracle session context requires additional setup
            return string.Empty;

            /* Future implementation example:
            if (httpContext is null)
            {
                return string.Empty;
            }

            Dictionary<string, string> sessionParams = AuthorizationResolver.GetProcessedUserClaims(httpContext);
            StringBuilder sessionQuery = new();
            
            foreach ((string claimType, string claimValue) in sessionParams)
            {
                // Oracle application context: DBMS_SESSION.SET_CONTEXT('namespace', 'attribute', 'value')
                string statement = $"DBMS_SESSION.SET_CONTEXT('DAB_CONTEXT', '{claimType}', '{claimValue}');";
                sessionQuery.Append(statement);
            }

            if (sessionQuery.Length > 0)
            {
                return "BEGIN " + sessionQuery.ToString() + " END;";
            }

            return string.Empty;
            */
        }

        /// <inheritdoc/>
        public override DbCommand PrepareDbCommand(
            OracleConnection conn,
            string sqltext,
            IDictionary<string, DbConnectionParam> parameters,
            HttpContext? httpContext,
            string dataSourceName)
        {
            OracleCommand cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.Text;

            string sessionParamsQuery = GetSessionParamsQuery(httpContext, parameters, dataSourceName);
            cmd.CommandText = sessionParamsQuery + sqltext;

            if (parameters is not null)
            {
                foreach (KeyValuePair<string, DbConnectionParam> parameterEntry in parameters)
                {
                    OracleParameter parameter = cmd.CreateParameter();
                    parameter.ParameterName = parameterEntry.Key;

                    // Handle output parameters (for RETURNING clause)
                    if (parameterEntry.Key.StartsWith(":out_"))
                    {
                        parameter.Direction = ParameterDirection.Output;
                        // Set appropriate size for output parameters
                        if (parameterEntry.Value.OracleDbType is not null)
                        {
                            parameter.OracleDbType = (OracleDbType)parameterEntry.Value.OracleDbType;
                            // Set size based on type
                            parameter.Size = GetOutputParameterSize(parameter.OracleDbType);
                        }
                        else if (parameterEntry.Value.DbType is not null)
                        {
                            parameter.DbType = (DbType)parameterEntry.Value.DbType;
                            parameter.Size = GetOutputParameterSizeFromDbType(parameter.DbType);
                        }
                    }
                    else
                    {
                        // Input parameters
                        parameter.Value = parameterEntry.Value.Value ?? DBNull.Value;
                        PopulateDbTypeForParameter(parameterEntry, parameter);
                    }

                    cmd.Parameters.Add(parameter);
                }
            }

            cmd.BindByName = true;
            return cmd;
        }

        /// <summary>
        /// Populates OracleDbType for Oracle parameters.
        /// </summary>
        public static void PopulateDbTypeForParameter(
            KeyValuePair<string, DbConnectionParam> parameterEntry,
            OracleParameter parameter)
        {
            if (parameterEntry.Value is not null)
            {
                if (parameterEntry.Value.DbType is not null)
                {
                    parameter.DbType = (DbType)parameterEntry.Value.DbType;
                }

                if (parameterEntry.Value.OracleDbType is not null)
                {
                    parameter.OracleDbType = (OracleDbType)parameterEntry.Value.OracleDbType;
                }
            }
        }

        /// <inheritdoc/>
        public override async Task<DbResultSet> GetMultipleResultSetsIfAnyAsync(
            DbDataReader dbDataReader,
            List<string>? args = null)
        {
            // For upsert operations using output parameters, the data comes from the parameters
            // not the DbDataReader, so check if this is a parameter-based result
            DbResultSet dbResultSet = await ExtractResultSetFromDbDataReaderAsync(dbDataReader);

            if (dbResultSet is null || dbResultSet.Rows.Count == 0)
            {
                if (args is not null && args.Count > 1)
                {
                    string prettyPrintPk = args[0];
                    string entityName = args[1];

                    throw new DataApiBuilderException(
                        message: $"Cannot perform operation on {entityName} with primary key {prettyPrintPk}.",
                        statusCode: HttpStatusCode.NotFound,
                        subStatusCode: DataApiBuilderException.SubStatusCodes.ItemNotFound);
                }

                throw new DataApiBuilderException(
                    message: "The operation could not be performed.",
                    statusCode: HttpStatusCode.InternalServerError,
                    subStatusCode: DataApiBuilderException.SubStatusCodes.UnexpectedError);
            }

            return dbResultSet;
        }

        /// <summary>
        /// Executes INSERT/UPDATE with RETURNING clause and extracts output values.
        /// </summary>
        public static async Task<DbResultSet> ExecuteInsertOrUpdateWithReturningAsync(
            OracleCommand cmd,
            List<LabelledColumn> outputColumns)
        {
            await cmd.ExecuteNonQueryAsync();

            DbResultSet resultSet = new(new Dictionary<string, object>());
            DbResultSetRow row = new();

            foreach (LabelledColumn column in outputColumns)
            {
                string paramName = $":out_{column.ColumnName}";
                if (cmd.Parameters.Contains(paramName))
                {
                    OracleParameter param = (OracleParameter)cmd.Parameters[paramName];
                    row.Columns.Add(column.Label, param.Value == DBNull.Value ? null : param.Value);
                }
            }

            resultSet.Rows.Add(row);
            return resultSet;
        }

        ///// <summary>
        ///// Extracts LabelledColumn information from output parameters in the command.
        ///// </summary>
        //private static List<LabelledColumn> ExtractOutputColumnsFromParameters(OracleParameterCollection parameters)
        //{
        //    List<LabelledColumn> outputColumns = new();

        //    foreach (OracleParameter param in parameters)
        //    {
        //        if (param.Direction == ParameterDirection.Output && param.ParameterName.StartsWith("out_", StringComparison.OrdinalIgnoreCase))
        //        {
        //            // Remove the "out_" prefix to get the column name
        //            string columnName = param.ParameterName.Substring(4);

        //            // For Oracle, the label and column name are typically the same
        //            // unless there's a mapping defined elsewhere
        //            // Updated to match the required LabelledColumn constructor signature
        //            outputColumns.Add(new LabelledColumn(columnName, columnName, columnName, columnName, null));
        //        }
        //    }

        //    return outputColumns;
        //}

        /// <summary>
        /// Determines appropriate size for output parameters based on OracleDbType.
        /// </summary>
        private static int GetOutputParameterSize(OracleDbType oracleDbType)
        {
            return oracleDbType switch
            {
                OracleDbType.Varchar2 => 4000,
                OracleDbType.NVarchar2 => 2000,
                OracleDbType.Clob => Int32.MaxValue,
                OracleDbType.NClob => Int32.MaxValue,
                OracleDbType.Blob => Int32.MaxValue,
                OracleDbType.Raw => 2000,
                OracleDbType.Date => 0,
                OracleDbType.TimeStamp => 0,
                OracleDbType.Int16 => 0,
                OracleDbType.Int32 => 0,
                OracleDbType.Int64 => 0,
                OracleDbType.Decimal => 0,
                OracleDbType.Double => 0,
                OracleDbType.Single => 0,
                _ => 4000 // Default size
            };
        }

        /// <summary>
        /// Determines appropriate size for output parameters based on DbType.
        /// </summary>
        private static int GetOutputParameterSizeFromDbType(DbType dbType)
        {
            return dbType switch
            {
                DbType.String => 4000,
                DbType.AnsiString => 4000,
                DbType.Binary => 2000,
                DbType.DateTime => 0,
                DbType.Date => 0,
                DbType.Time => 0,
                DbType.Int16 => 0,
                DbType.Int32 => 0,
                DbType.Int64 => 0,
                DbType.Decimal => 0,
                DbType.Double => 0,
                DbType.Single => 0,
                DbType.Boolean => 0,
                DbType.Guid => 0,
                _ => 4000
            };
        }
    }
}
