// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Azure.DataApiBuilder.Service.Tests.Authorization.GraphQL.Policies.Mutation.Delete
{
    /// <summary>
    /// Tests Database Authorization Policies applied to GraphQL Queries
    /// </summary>
    [TestClass, TestCategory(TestCategory.ORACLE)]
    public class OracleGraphQLDeleteMutationPolicyTests : GraphQLDeleteMutationDatabasePolicyTestBase
    {
        /// <summary>
        /// Set the database engine for the tests
        /// </summary>
        [ClassInitialize]
        public static async Task SetupAsync(TestContext context)
        {
            DatabaseEngine = TestCategory.ORACLE;
            await InitializeTestFixture();
        }

        /// <summary>
        /// Tests Authenticated GraphQL Delete Mutation which triggers
        /// policy processing. Tests deleteBook with policy that
        /// allows/prevents operation.
        /// - Operation allowed: confirm record deleted.
        /// - Operation forbidden: confirm record not deleted.
        /// </summary>
        [TestMethod]
        public async Task DeleteMutation_Policy()
        {
            string dbQuery = @"
                SELECT JSON_OBJECT(
                'id' VALUE table0.id,
                'title' VALUE table0.title
                ABSENT ON NULL
                ) as json_result
                FROM books table0
                WHERE table0.id = 9 
                AND table0.title = 'Policy-Test-01'
                ORDER BY table0.id ASC
                FETCH FIRST 1 ROWS ONLY;";

            await DeleteMutation_Policy(dbQuery);
        }
    }
}
