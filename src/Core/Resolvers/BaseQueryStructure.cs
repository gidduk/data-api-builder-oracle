// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using Azure.DataApiBuilder.Auth;
using Azure.DataApiBuilder.Config.DatabasePrimitives;
using Azure.DataApiBuilder.Config.ObjectModel;
using Azure.DataApiBuilder.Core.Models;
using Azure.DataApiBuilder.Core.Services;
using Azure.DataApiBuilder.Core.Services.MetadataProviders;
using Azure.DataApiBuilder.Service.Exceptions;
using Azure.DataApiBuilder.Service.GraphQLBuilder.Queries;
using HotChocolate.Language;

namespace Azure.DataApiBuilder.Core.Resolvers
{
    public class BaseQueryStructure
    {
        /// <summary>
        /// The Entity name associated with this query as appears in the config file.
        /// </summary>
        public virtual string EntityName { get; set; }

        /// <summary>
        /// The alias of the entity as used in the generated query.
        /// </summary>
        public virtual string SourceAlias { get; set; }

        /// <summary>
        /// The metadata provider of the respective database.
        /// </summary>
        public ISqlMetadataProvider MetadataProvider { get; }

        /// <summary>
        /// The DatabaseObject associated with the entity, represents the
        /// database object to be queried.
        /// </summary>
        public DatabaseObject DatabaseObject { get; protected set; } = null!;

        /// <summary>
        /// The columns which the query selects
        /// </summary>
        public List<LabelledColumn> Columns { get; }

        /// <summary>
        /// Counter.Next() can be used to get a unique integer within this
        /// query, which can be used to create unique aliases, parameters or
        /// other identifiers.
        /// </summary>
        public IncrementingInteger Counter { get; }

        /// <summary>
        /// Parameters values required to execute the query.
        /// </summary>
        public Dictionary<string, DbConnectionParam> Parameters { get; set; }

        /// <summary>
        /// Predicates that should filter the result set of the query.
        /// </summary>
        public List<Predicate> Predicates { get; }

        /// <summary>
        /// Used for parsing GraphQL filter arguments.
        /// </summary>
        public GQLFilterParser GraphQLFilterParser { get; protected set; }

        /// <summary>
        /// Authorization Resolver used within SqlQueryStructure to get and apply
        /// authorization policies to requests.
        /// </summary>
        public IAuthorizationResolver AuthorizationResolver { get; }

        /// <summary>
        /// DbPolicyPredicates is a string that represents the filter portion of our query
        /// in the WHERE Clause added by virtue of the database policy.
        /// </summary>
        public virtual Dictionary<EntityActionOperation, string?> DbPolicyPredicatesForOperations { get; set; } = new();

        public const string PARAM_NAME_PREFIX = "@";
        public const string PARAM_NAME_PREFIX_ORACLE = ":";

        public BaseQueryStructure(
            ISqlMetadataProvider metadataProvider,
            IAuthorizationResolver authorizationResolver,
            GQLFilterParser gQLFilterParser,
            List<Predicate>? predicates = null,
            string entityName = "",
            IncrementingInteger? counter = null)
        {
            Columns = new();
            Parameters = new();
            Predicates = predicates ?? new();
            Counter = counter ?? new IncrementingInteger();
            MetadataProvider = metadataProvider;
            GraphQLFilterParser = gQLFilterParser;
            AuthorizationResolver = authorizationResolver;

            // Default the alias to the empty string since this base constructor
            // is called for requests other than Find operations. We only use
            // SourceAlias for Find, so we leave empty here and then populate
            // in the Find specific contractor.
            SourceAlias = string.Empty;

            if (!string.IsNullOrEmpty(entityName))
            {
                EntityName = entityName;
                DatabaseObject = MetadataProvider.EntityToDatabaseObject[entityName];
            }
            else
            {
                EntityName = string.Empty;
                DatabaseObject = new DatabaseTable(schemaName: string.Empty, tableName: string.Empty);
            }
        }

        /// <summary>
        ///  Add parameter to Parameters and return the name associated with it
        /// </summary>
        /// <param name="value">Value to be assigned to parameter, which can be null for nullable columns.</param>
        /// <param name="paramName"> The name of the parameter - backing column name for table/views or parameter name for stored procedures.</param>
        public virtual string MakeDbConnectionParam(object? value, string? paramName = null)
        {
            string encodedParamName = MetadataProvider is OracleMetadataProvider ? GetEncodedParamNameForOracle(Counter.Next()) : GetEncodedParamName(Counter.Next());
            if (!string.IsNullOrEmpty(paramName))
            {
                Parameters.Add(encodedParamName,
                    new(value,
                        dbType: GetUnderlyingSourceDefinition().GetDbTypeForParam(paramName),
                        sqlDbType: GetUnderlyingSourceDefinition().GetSqlDbTypeForParam(paramName)));
            }
            else
            {
                // Add these parameters only if its not oracle, Oracle requires parameters to be available only when the query requires whem else it will throw ORA-01006: Bind variable does not exist
                if (MetadataProvider is not OracleMetadataProvider)
                {
                    Parameters.Add(encodedParamName, new(value));
                }
            }

            return encodedParamName;
        }

        /// <summary>
        /// Helper method to create encoded parameter name.
        /// </summary>
        /// <param name="counterValue">The counter value used as a suffix in the encoded parameter name.</param>
        /// <returns>Encoded parameter name.</returns>
        public static string GetEncodedParamName(ulong counterValue)
        {
            return  $"{PARAM_NAME_PREFIX}param{counterValue}";
        }

        /// <summary>
        /// Helper method to create encoded parameter name for Oracle
        /// </summary>
        /// <param name="counterValue">The counter value used as a suffix in the encoded parameter name.</param>
        /// <returns>Encoded parameter name.</returns>
        public static string GetEncodedParamNameForOracle(ulong counterValue)
        {
            return $"{PARAM_NAME_PREFIX_ORACLE}param{counterValue}";
        }

        /// <summary>
        /// Creates a unique table alias.
        /// </summary>
        public string CreateTableAlias()
        {
            return $"table{Counter.Next()}";
        }

        /// <summary>
        /// Returns the SourceDefinitionDefinition for the entity(table/view) of this query.
        /// </summary>
        public virtual SourceDefinition GetUnderlyingSourceDefinition()
        {
            return MetadataProvider.GetSourceDefinition(EntityName);
        }

        /// <summary>
        /// Extracts the *Connection.items query field from the *Connection query field
        /// </summary>
        /// <returns> The query field or null if **Conneciton.items is not requested in the query</returns>
        internal static FieldNode? ExtractQueryField(FieldNode connectionQueryField)
        {
            FieldNode? itemsField = null;
            FieldNode? groupByField = null;
            foreach (ISelectionNode node in connectionQueryField.SelectionSet!.Selections)
            {
                FieldNode field = (FieldNode)node;
                string fieldName = field.Name.Value;

                if (fieldName == QueryBuilder.PAGINATION_FIELD_NAME)
                {
                    itemsField = field;
                }
                else if (fieldName == QueryBuilder.GROUP_BY_FIELD_NAME)
                {
                    groupByField = field;
                }
            }

            if (itemsField != null && groupByField != null)
            {
                // This is temporary and iteratively we will allow both items and groupby in same query.
                throw new DataApiBuilderException(
                    message: "Cannot have both groupBy and items in the same query",
                    statusCode: HttpStatusCode.ServiceUnavailable,
                    subStatusCode: DataApiBuilderException.SubStatusCodes.BadRequest);
            }

            return groupByField is null ? itemsField : groupByField;
        }

        /// <summary>
        /// Extracts the *Connection.items schema field from the *Connection schema field
        /// </summary>
        internal static IObjectField ExtractItemsSchemaField(IObjectField connectionSchemaField)
        {
            return connectionSchemaField.Type.NamedType<ObjectType>().Fields[QueryBuilder.PAGINATION_FIELD_NAME];
        }
    }
}
