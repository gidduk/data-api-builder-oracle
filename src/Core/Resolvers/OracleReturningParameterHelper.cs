// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.DataApiBuilder.Config.DatabasePrimitives;
using Azure.DataApiBuilder.Core.Models;

namespace Azure.DataApiBuilder.Core.Resolvers
{
    /// <summary>
    /// Helper class for managing Oracle RETURNING clause output parameters.
    /// Provides utilities to add output parameters required by Oracle's RETURNING clause.
    /// </summary>
    internal sealed class OracleReturningParameterHelper
    {
        /// <summary>
        /// Adds output parameters for Oracle RETURNING clause to the parameters dictionary.
        /// Oracle requires output parameters to be created with proper direction and size
        /// before executing the command with RETURNING clause.
        /// </summary>
        /// <param name="outputColumns">The columns that will be returned by the RETURNING clause.</param>
        /// <param name="sourceDefinition">The source definition containing column metadata.</param>
        /// <param name="parameters">The parameters dictionary to add output parameters to.</param>
        public static void AddOracleOutputParameters(
            List<LabelledColumn> outputColumns,
            SourceDefinition sourceDefinition,
            IDictionary<string, DbConnectionParam> parameters)
        {
            foreach (LabelledColumn outputColumn in outputColumns)
            {
                string outputParamName = $":out_{outputColumn.ColumnName}";

                // Only add if not already present
                if (!parameters.ContainsKey(outputParamName))
                {
                    if (sourceDefinition.Columns.TryGetValue(outputColumn.ColumnName, out ColumnDefinition? columnDef))
                    {
                        parameters.Add(outputParamName, new DbConnectionParam(
                            value: null, // Output parameters start as null
                            dbType: columnDef.DbType,
                            sqlDbType: null, // Oracle doesn't use SqlDbType
                            oracleDbType: columnDef.OracleDbType
                        ));
                    }
                }
            }
        }
    }
}
