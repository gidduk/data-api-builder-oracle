// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Data.Common;
using System.Net;
using Azure.DataApiBuilder.Core.Configurations;
using Oracle.ManagedDataAccess.Client;

namespace Azure.DataApiBuilder.Core.Resolvers
{
    /// <summary>
    /// Class to handle database specific logic for exception handling for Oracle.
    /// <seealso cref="https://docs.oracle.com/en/database/oracle/oracle-database/19/errmg/database-error-messages.pdf"/>
    /// </summary>
    public class OracleExceptionParser : DbExceptionParser
    {
        public OracleExceptionParser(RuntimeConfigProvider configProvider) : base(configProvider)
        {
            // HashSet of 'SqlState'(s) which are to be considered as bad requests.
            BadRequestExceptionCodes.UnionWith(new List<string>
            {
                // ORA-00001 unique constraint (string.string) violated
                "00001",
                // ORA-02266 unique/primary keys in table referenced by enabled foreign keys
                "02266",
                // ORA-01451 column to be modified to NULL cannot be modified to NULL
                "01451",
                // ORA-02290 check constraint (string.string) violated
                "02290",
                // ORA-02291 integrity constraint (string.string) violated - parent key not found
                "02291",
                // ORA-02292 integrity constraint (string.string) violated - child record found
                "02292",
                // ORA-02293 cannot validate (string.string) - check constraint violated
                "02293",
                // ORA-02299 cannot validate (string.string) - duplicate keys found
                "02299",
                // ORA-02428 could not add foreign key reference
                "02428",
                // ORA-01761 DML operation does not map to a unique table in the join
                "01761",
                // ORA-02290 check constraint (string.string) violated
                "02267",
                // ORA-06550 PL/SQL: Compilation error
                "06550",
                // ORA-01422 - TOO_MANY_ROWS - exact fetch returns more than requested number of rows
                "01422",
                // ORA-06504 - PL/SQL: Return types of Result Set Mismatch
                "06504",
                // ORA-30656 - Invalid REF cursor type declaration
                "30656",
                // ORA-01017 - invalid username/password; logon denied
                "01017",
                // ORA-01403 - no data found
                "01403",
                // ORA-06592 - CASE_NOT_FOUND
                "06592",
                // ORA-01476 - ZERO_DIVIDE - divisor is equal to zero
                "01476"

            });

            TransientExceptionCodes.UnionWith(new List<string>
            {
                "00054", "00060", "01033", "01034", "03113", "03135", "12170", "12170"
            });

            ConflictExceptionCodes.UnionWith(new List<string>
            {
                "00001", "01400", "02291", "02291"
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
