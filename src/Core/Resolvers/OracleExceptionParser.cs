// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Data.Common;
using System.Net;
using Azure.DataApiBuilder.Core.Configurations;
using Oracle.ManagedDataAccess.Client;

namespace Azure.DataApiBuilder.Core.Resolvers
{
    /// <summary>
    /// Class to handle database specific logic for exception handling for MySql.
    /// <seealso cref="https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html"/>
    /// </summary>
    public class OracleExceptionParser : DbExceptionParser
    {
        public OracleExceptionParser(RuntimeConfigProvider configProvider) : base(configProvider)
        {
            // HashSet of 'SqlState'(s) which are to be considered as bad requests.
            BadRequestExceptionCodes.UnionWith(new List<string>
            {
                // ER_BAD_NULL_ERROR : Column '%s' cannot be null
                "1048",
                // 2.  ER_NON_UNIQ_ERROR: Column '%s' in %s is ambiguous
                "1052",
                // ORA-00001 unique constraint (string.string) violated
                "00001",
                // ER_NO_REFERENCED_ROW: Cannot add or update a child row: a foreign key constraint fails
                "1216",
                // ORA-02266 unique/primary keys in table referenced by enabled foreign keys
                "02266",
                // ER_ROW_IS_REFERENCED_2: Cannot delete or update a parent row: a foreign key constraint fails (%s)
                "1451",
                // ORA-02291 integrity constraint (string.string) violated - parent key not found
                "02291",
                // ORA-02292 integrity constraint (string.string) violated - child record found
                "02292",
                // ORA-02299 cannot validate (string.string) - duplicate keys found
                "02299",
                // ORA-02428 could not add foreign key reference
                "02428",
                // ER_FOREIGN_DUPLICATE_KEY_WITH_CHILD_INFO: Foreign key constraint for table '%s', record '%s' would
                // lead to a duplicate entry in table '%s', key '%s'
                "1761",
                // ER_FOREIGN_DUPLICATE_KEY_WITHOUT_CHILD_INFO: Foreign key constraint for table '%s', record '%s' would
                // lead to a duplicate entry in a child table
                "1762",
                // ORA-02290 check constraint (string.string) violated
                "02290",
                // ORA-02267 column type incompatible with referenced column type
                "02267"
            });

            TransientExceptionCodes.UnionWith(new List<string>
            {
                // List compiled from: https://mariadb.com/kb/en/mariadb-error-codes/
                "1020", "1021", "1037", "1038", "1040", "1041", "1150", "1151", "1156", "1157",
                "1158", "1159", "1160", "1161", "1192", "1203", "1205", "1206", "1223"
            });

            ConflictExceptionCodes.UnionWith(new List<string>
            {
                "1022", "1062", "1223", "1586", "1706", "1934"
            });
        }

        /// <inheritdoc/>
        public override bool IsTransientException(DbException e)
        {
            OracleException ex = (OracleException)e;
            return TransientExceptionCodes.Contains(ex.Number.ToString());
        }

        /// <inheritdoc/>
        public override HttpStatusCode GetHttpStatusCodeForException(DbException e)
        {
            OracleException ex = (OracleException)e;
            string exceptionNumber = ex.Number.ToString();

            if (BadRequestExceptionCodes.Contains(exceptionNumber))
            {
                return HttpStatusCode.BadRequest;
            }

            if (ConflictExceptionCodes.Contains(exceptionNumber))
            {
                return HttpStatusCode.Conflict;
            }

            return HttpStatusCode.InternalServerError;
        }
    }
}
