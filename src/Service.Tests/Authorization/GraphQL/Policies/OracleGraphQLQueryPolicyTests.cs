// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Azure.DataApiBuilder.Service.Tests.Authorization.GraphQL.Policies
{
    /// <summary>
    /// Tests Database Authorization Policies applied to GraphQL Queries
    /// </summary>
    [TestClass, TestCategory(TestCategory.ORACLE)]
    public class OracleGraphQLQueryPolicyTests : GraphQLQueryDatabasePolicyTestBase
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
        /// Tests Authenticated GraphQL Queries which trigger
        /// policy processing. Tests QueryByPK with policies that
        /// filter results:
        /// - To 0 records to detect expected null result
        /// - To 1 record to validate result returns as expected.
        /// </summary>
        [TestMethod]
        public async Task QueryByPK_Policy()
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

            await QueryByPK_Policy(dbQuery);
        }

        /// <summary>
        /// Tests a GraphQL query that may fetch multiple result records,
        /// but does not include any nested queries.
        /// When a policy is applied to such top-level query, results are restricted
        /// to the expected records.
        /// </summary>
        [TestMethod]
        public async Task QueryMany_Policy()
        {
            // Tests Book Read Policy: @item.title ne 'Policy-Test-01'
            // Due to restrictive book policy, expects all book records except:
            // id: 9 title: 'Policy-Test-01'
            string dbQuery = @"
                            SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                    'id' VALUE table0.id,
                    'title' VALUE table0.title
                    ABSENT ON NULL
                )
            ) as json_result
            FROM (
                SELECT 
                    table0.id,
                    table0.title
                FROM books table0
                WHERE table0.title != 'Policy-Test-01'
                ORDER BY table0.id ASC
                FETCH FIRST 100 ROWS ONLY
            ) table0;";

            string clientRole = "policy_tester_02";
            await QueryMany_Policy(dbQuery, clientRole);

            // Tests Book Read Policy: @item.title eq 'Policy-Test-01'
            // Due to restrictive book policy, expects one book result:
            // id: 9 title: 'Policy-Test-01'
            string dbQuery_restrictToOneResult = @"
                                SELECT JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'id' VALUE table0.id,
                        'title' VALUE table0.title
                        ABSENT ON NULL
                    )
                ) as json_result
                FROM (
                    SELECT 
                        table0.id,
                        table0.title
                    FROM books table0
                    WHERE table0.title = 'Policy-Test-01'
                    ORDER BY table0.id ASC
                    FETCH FIRST 100 ROWS ONLY
                ) table0;";

            clientRole = "policy_tester_01";
            await QueryMany_Policy(dbQuery_restrictToOneResult, clientRole);
        }

        /// <summary>
        /// Tests a GraphQL query that may fetch multiple result records
        /// on a table with a nullable field, but does not include any nested queries.
        /// When a policy is applied to such top-level query, results are restricted
        /// to the expected records.
        /// </summary>
        [TestMethod]
        public async Task QueryMany_Policy_Nullable()
        {
            string dbQuery = @"
                            SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                    'speciesid' VALUE table0.speciesid,
                    'region' VALUE table0.region
                    ABSENT ON NULL
                )
            ) as json_result
            FROM (
                SELECT 
                    table0.speciesid,
                    table0.region
                FROM fungi table0
                WHERE table0.region != 'northeast'
                ORDER BY table0.speciesid ASC
                FETCH FIRST 100 ROWS ONLY
            ) table0;";

            await QueryMany_Policy_Nullable(dbQuery);
        }

        [TestMethod]
        public async Task QueryMany_NestedRequest_Policy()
        {
            string dbQuery = @"
                            SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                    'id' VALUE table0.id,
                    'title' VALUE table0.title,
                    'publishers' VALUE JSON_QUERY(table1_subq.data)
                    ABSENT ON NULL
                )
            ) as json_result
            FROM books table0
            OUTER APPLY (
                SELECT JSON_OBJECT(
                    'id' VALUE table1.id,
                    'name' VALUE table1.name
                    ABSENT ON NULL
                ) as data
                FROM (
                    SELECT 
                        table1.id,
                        table1.name
                    FROM publishers table1
                    WHERE table1.id = 1940 
                    AND table0.publisher_id = table1.id
                    ORDER BY table1.id ASC
                    FETCH FIRST 1 ROWS ONLY
                ) table1
            ) table1_subq
            WHERE table0.title = 'Policy-Test-01'
            ORDER BY table0.id ASC
            FETCH FIRST 100 ROWS ONLY;";

            // Tests Book Read Policy: @item.title eq 'Policy-Test-01'
            // Publisher Read Policy: @item.id ne 1940
            // Expects HotChocolate error since nested query fails to resolve
            // at least one publisher record due to restrictive policy.
            await QueryMany_NestedRequest_Policy(
                dbQuery,
                roleName: "policy_tester_03",
                expectError: true);

            // Tests Book Read Policy: @item.title eq 'Policy-Test-01'
            // Publisher Read Policy: @item.id eq 1940
            // Target Record: id: 9, title: 'Policy-Test-01' publisher_id: 1940
            // The top-level book policy restricts this result to one record while
            // the nested query policy resolves at least one result, avoiding
            // resolving null for a non-nullable field.
            await QueryMany_NestedRequest_Policy(
                dbQuery,
                roleName: "policy_tester_01",
                expectError: false);
        }
    }
}
