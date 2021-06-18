// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catconstants

import "math"

// ReportableAppNamePrefix indicates that the application name can be
// reported in telemetry without scrubbing. (Note this only applies to
// the application name itself. Query data is still scrubbed as
// usual.)
const ReportableAppNamePrefix = "$ "

// InternalAppNamePrefix indicates that the application name identifies
// an internal task / query / job to CockroachDB. Different application
// names are used to classify queries in different categories.
const InternalAppNamePrefix = ReportableAppNamePrefix + "internal"

// DelegatedAppNamePrefix is added to a regular client application
// name for SQL queries that are ran internally on behalf of other SQL
// queries inside that application. This is not the same as
// RepotableAppNamePrefix; in particular the application name with
// DelegatedAppNamePrefix should be scrubbed in reporting.
const DelegatedAppNamePrefix = "$$ "

// Oid for virtual database and table.
const (
	CrdbInternalID = math.MaxUint32 - iota
	CrdbInternalBackwardDependenciesTableID
	CrdbInternalBuildInfoTableID
	CrdbInternalBuiltinFunctionsTableID
	CrdbInternalClusterContendedIndexesViewID
	CrdbInternalClusterContendedKeysViewID
	CrdbInternalClusterContendedTablesViewID
	CrdbInternalClusterContentionEventsTableID
	CrdbInternalClusterDistSQLFlowsTableID
	CrdbInternalClusterQueriesTableID
	CrdbInternalClusterTransactionsTableID
	CrdbInternalClusterSessionsTableID
	CrdbInternalClusterSettingsTableID
	CrdbInternalCreateStmtsTableID
	CrdbInternalCreateTypeStmtsTableID
	CrdbInternalDatabasesTableID
	CrdbInternalFeatureUsageID
	CrdbInternalForwardDependenciesTableID
	CrdbInternalKVNodeLivenessTableID
	CrdbInternalGossipNodesTableID
	CrdbInternalGossipAlertsTableID
	CrdbInternalGossipLivenessTableID
	CrdbInternalGossipNetworkTableID
	CrdbInternalIndexColumnsTableID
	CrdbInternalIndexUsageStatisticsTableID
	CrdbInternalInflightTraceSpanTableID
	CrdbInternalJobsTableID
	CrdbInternalKVNodeStatusTableID
	CrdbInternalKVStoreStatusTableID
	CrdbInternalLeasesTableID
	CrdbInternalLocalContentionEventsTableID
	CrdbInternalLocalDistSQLFlowsTableID
	CrdbInternalLocalQueriesTableID
	CrdbInternalLocalTransactionsTableID
	CrdbInternalLocalSessionsTableID
	CrdbInternalLocalMetricsTableID
	CrdbInternalPartitionsTableID
	CrdbInternalPredefinedCommentsTableID
	CrdbInternalRangesNoLeasesTableID
	CrdbInternalRangesViewID
	CrdbInternalRuntimeInfoTableID
	CrdbInternalSchemaChangesTableID
	CrdbInternalSessionTraceTableID
	CrdbInternalSessionVariablesTableID
	CrdbInternalStmtStatsTableID
	CrdbInternalTableColumnsTableID
	CrdbInternalTableIndexesTableID
	CrdbInternalTablesTableID
	CrdbInternalTablesTableLastStatsID
	CrdbInternalTransactionStatsTableID
	CrdbInternalTxnStatsTableID
	CrdbInternalZonesTableID
	CrdbInternalInvalidDescriptorsTableID
	CrdbInternalClusterDatabasePrivilegesTableID
	CrdbInternalInterleaved
	CrdbInternalCrossDbRefrences
	CrdbInternalLostTableDescriptors
	CrdbInternalClusterInflightTracesTable
	CrdbInternalRegionsTable
	InformationSchemaID
	InformationSchemaAdministrableRoleAuthorizationsID
	InformationSchemaApplicableRolesID
	InformationSchemaAttributesTableID
	InformationSchemaCharacterSets
	InformationSchemaCheckConstraintRoutineUsageTableID
	InformationSchemaCheckConstraints
	InformationSchemaCollationCharacterSetApplicability
	InformationSchemaCollations
	InformationSchemaColumnColumnUsageTableID
	InformationSchemaColumnDomainUsageTableID
	InformationSchemaColumnOptionsTableID
	InformationSchemaColumnPrivilegesID
	InformationSchemaColumnStatisticsTableID
	InformationSchemaColumnUDTUsageID
	InformationSchemaColumnsExtensionsTableID
	InformationSchemaColumnsTableID
	InformationSchemaConstraintColumnUsageTableID
	InformationSchemaConstraintTableUsageTableID
	InformationSchemaDataTypePrivilegesTableID
	InformationSchemaDomainConstraintsTableID
	InformationSchemaDomainUdtUsageTableID
	InformationSchemaDomainsTableID
	InformationSchemaElementTypesTableID
	InformationSchemaEnabledRolesID
	InformationSchemaEnginesTableID
	InformationSchemaEventsTableID
	InformationSchemaFilesTableID
	InformationSchemaForeignDataWrapperOptionsTableID
	InformationSchemaForeignDataWrappersTableID
	InformationSchemaForeignServerOptionsTableID
	InformationSchemaForeignServersTableID
	InformationSchemaForeignTableOptionsTableID
	InformationSchemaForeignTablesTableID
	InformationSchemaInformationSchemaCatalogNameTableID
	InformationSchemaKeyColumnUsageTableID
	InformationSchemaKeywordsTableID
	InformationSchemaOptimizerTraceTableID
	InformationSchemaParametersTableID
	InformationSchemaPartitionsTableID
	InformationSchemaPluginsTableID
	InformationSchemaProcesslistTableID
	InformationSchemaProfilingTableID
	InformationSchemaReferentialConstraintsTableID
	InformationSchemaResourceGroupsTableID
	InformationSchemaRoleColumnGrantsTableID
	InformationSchemaRoleRoutineGrantsTableID
	InformationSchemaRoleTableGrantsID
	InformationSchemaRoleUdtGrantsTableID
	InformationSchemaRoleUsageGrantsTableID
	InformationSchemaRoutinePrivilegesTableID
	InformationSchemaRoutineTableID
	InformationSchemaSQLFeaturesTableID
	InformationSchemaSQLImplementationInfoTableID
	InformationSchemaSQLPartsTableID
	InformationSchemaSQLSizingTableID
	InformationSchemaSchemataExtensionsTableID
	InformationSchemaSchemataTableID
	InformationSchemaSchemataTablePrivilegesID
	InformationSchemaSequencesID
	InformationSchemaSessionVariables
	InformationSchemaStGeometryColumnsTableID
	InformationSchemaStSpatialReferenceSystemsTableID
	InformationSchemaStUnitsOfMeasureTableID
	InformationSchemaStatisticsTableID
	InformationSchemaTableConstraintTableID
	InformationSchemaTableConstraintsExtensionsTableID
	InformationSchemaTablePrivilegesID
	InformationSchemaTablesExtensionsTableID
	InformationSchemaTablesTableID
	InformationSchemaTablespacesExtensionsTableID
	InformationSchemaTablespacesTableID
	InformationSchemaTransformsTableID
	InformationSchemaTriggeredUpdateColumnsTableID
	InformationSchemaTriggersTableID
	InformationSchemaTypePrivilegesID
	InformationSchemaUdtPrivilegesTableID
	InformationSchemaUsagePrivilegesTableID
	InformationSchemaUserAttributesTableID
	InformationSchemaUserDefinedTypesTableID
	InformationSchemaUserMappingOptionsTableID
	InformationSchemaUserMappingsTableID
	InformationSchemaUserPrivilegesID
	InformationSchemaViewColumnUsageTableID
	InformationSchemaViewRoutineUsageTableID
	InformationSchemaViewTableUsageTableID
	InformationSchemaViewsTableID
	PgCatalogID
	PgCatalogAggregateTableID
	PgCatalogAmTableID
	PgCatalogAmopTableID
	PgCatalogAmprocTableID
	PgCatalogAttrDefTableID
	PgCatalogAttributeTableID
	PgCatalogAuthIDTableID
	PgCatalogAuthMembersTableID
	PgCatalogAvailableExtensionVersionsTableID
	PgCatalogAvailableExtensionsTableID
	PgCatalogCastTableID
	PgCatalogClassTableID
	PgCatalogCollationTableID
	PgCatalogConfigTableID
	PgCatalogConstraintTableID
	PgCatalogConversionTableID
	PgCatalogCursorsTableID
	PgCatalogDatabaseTableID
	PgCatalogDbRoleSettingTableID
	PgCatalogDefaultACLTableID
	PgCatalogDependTableID
	PgCatalogDescriptionTableID
	PgCatalogEnumTableID
	PgCatalogEventTriggerTableID
	PgCatalogExtensionTableID
	PgCatalogFileSettingsTableID
	PgCatalogForeignDataWrapperTableID
	PgCatalogForeignServerTableID
	PgCatalogForeignTableTableID
	PgCatalogGroupTableID
	PgCatalogHbaFileRulesTableID
	PgCatalogIndexTableID
	PgCatalogIndexesTableID
	PgCatalogInheritsTableID
	PgCatalogInitPrivsTableID
	PgCatalogLanguageTableID
	PgCatalogLargeobjectMetadataTableID
	PgCatalogLargeobjectTableID
	PgCatalogLocksTableID
	PgCatalogMatViewsTableID
	PgCatalogNamespaceTableID
	PgCatalogOpclassTableID
	PgCatalogOperatorTableID
	PgCatalogOpfamilyTableID
	PgCatalogPartitionedTableTableID
	PgCatalogPoliciesTableID
	PgCatalogPolicyTableID
	PgCatalogPreparedStatementsTableID
	PgCatalogPreparedXactsTableID
	PgCatalogProcTableID
	PgCatalogPublicationRelTableID
	PgCatalogPublicationTableID
	PgCatalogPublicationTablesTableID
	PgCatalogRangeTableID
	PgCatalogReplicationOriginStatusTableID
	PgCatalogReplicationOriginTableID
	PgCatalogReplicationSlotsTableID
	PgCatalogRewriteTableID
	PgCatalogRolesTableID
	PgCatalogRulesTableID
	PgCatalogSecLabelsTableID
	PgCatalogSecurityLabelTableID
	PgCatalogSequenceTableID
	PgCatalogSequencesTableID
	PgCatalogSettingsTableID
	PgCatalogShadowTableID
	PgCatalogSharedDescriptionTableID
	PgCatalogSharedSecurityLabelTableID
	PgCatalogShdependTableID
	PgCatalogShmemAllocationsTableID
	PgCatalogStatActivityTableID
	PgCatalogStatisticExtTableID
	PgCatalogSubscriptionRelTableID
	PgCatalogSubscriptionTableID
	PgCatalogTablesTableID
	PgCatalogTablespaceTableID
	PgCatalogTimezoneAbbrevsTableID
	PgCatalogTimezoneNamesTableID
	PgCatalogTransformTableID
	PgCatalogTriggerTableID
	PgCatalogTsConfigMapTableID
	PgCatalogTsConfigTableID
	PgCatalogTsDictTableID
	PgCatalogTsParserTableID
	PgCatalogTsTemplateTableID
	PgCatalogTypeTableID
	PgCatalogUserMappingTableID
	PgCatalogUserMappingsTableID
	PgCatalogUserTableID
	PgCatalogViewsTableID
	PgExtensionSchemaID
	PgExtensionGeographyColumnsTableID
	PgExtensionGeometryColumnsTableID
	PgExtensionSpatialRefSysTableID
	MinVirtualID = PgExtensionSpatialRefSysTableID
)
