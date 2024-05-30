"""Stream class for tap-powerbi-metadata."""

from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib import parse
import requests
import json

from tap_powerbi_metadata.client import TapPowerBIMetadataStream, TapPowerBIUsageStream
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

class ActivityEventsStream(TapPowerBIUsageStream):
    """Returns a list of audit activity events for a tenant.
    Docs: https://learn.microsoft.com/en-us/rest/api/power-bi/admin/get-activity-events

    """
    name = "ActivityEvents"
    path = "/admin/activityevents"
    primary_keys = ["Id"]
    replication_key = "CreationTime"
    schema = PropertiesList(
        # Keys
        Property("Id", StringType, required=True),
        Property("CreationTime", DateTimeType, required=True),
        # Properties
        Property("AccessRequestMessage", StringType),
        Property("AccessRequestType", StringType),
        Property("Activity", StringType),
        Property("ActivityId", StringType),
        Property(
            "AggregatedWorkspaceInformation",
            ObjectType(
                Property("WorkspaceCount", IntegerType),
                Property("WorkspacesByCapacitySku", StringType),
                Property("WorkspacesByType", StringType),
            )
        ),
        Property("AppName", StringType),
        Property("AppId", StringType),
        Property("AppReportId", StringType),
        Property(
            "ArtifactAccessRequestInfo",
            ObjectType(
                Property("AccessRequestAction", StringType),
                Property("ArtifactLocationObjectId", StringType),
                Property(
                    "ArtifactOwnerInformation",
                    ArrayType(
                        ObjectType(
                            Property("EmailAddress", StringType),
                            Property("UserObjectId", StringType),
                        )
                    )
                ),
                Property("RequesterUserObjectId", StringType),
                Property("RequestId", IntegerType),
                Property("TenantObjectId", StringType),
                Property("WorkspaceName", StringType),
            )
        ),
        Property("ArtifactId", StringType),
        Property("ArtifactKind", StringType),
        Property("ArtifactName", StringType),
        Property("ArtifactObjectId", StringType),
        Property("AuditedArtifactInformation",
            ObjectType(
                Property("AnnotatedItemType", StringType),
                Property("ArtifactObjectId", StringType),
                Property("Id", StringType),
                Property("Name", StringType),
            )
        ),
        Property("CapacityId", StringType),
        Property("CapacityName", StringType),
        Property("CapacityState", StringType),
        Property("CapacityUsers", StringType),
        Property("ClientIP", StringType),
        Property("ConsumptionMethod", StringType),
        Property("CopiedReportId", StringType),
        Property("CopiedReportName", StringType),
        Property("CredentialSetupMode", StringType),
        Property("CustomVisualAccessTokenResourceId", StringType),
        Property("CustomVisualAccessTokenSiteUri", StringType),
        Property("DashboardId", StringType),
        Property("DashboardName", StringType),
        Property("DataConnectivityMode", StringType),
        Property(
            "DataflowAccessTokenRequestParameters",
            ObjectType(
                Property("entityName", StringType),
                Property("partitionUri", StringType),
                Property("permissions", IntegerType),
                Property("tokenLifetimeInMinutes", IntegerType),
            )
        ),
        Property("DataflowAllowNativeQueries", BooleanType),
        Property("DataflowId", StringType),
        Property("DataflowName", StringType),
        Property("DataflowRefreshScheduleType", StringType),
        Property("DataflowType", StringType),
        Property("DatasetCertificationStage", StringType),
        Property("DatasetId", StringType),
        Property("DatasetName", StringType),
        Property(
            "Datasets",
            ArrayType(
                ObjectType(
                    Property("DatasetId", StringType),
                    Property("DatasetName", StringType),
                )
            )
        ),
        Property("DatasourceId", StringType),
        Property("DatasourceDetails", BooleanType),
        Property("DatasourceInformations", ArrayType(
                ObjectType(
                    Property("CredentialType", StringType),
                    Property("DatasourceObjectId", StringType),
                    Property("DatasourceReference", StringType),
                    Property("GatewayObjectId", StringType),
                    Property("SingleSignOnType", StringType),
                )
            )
        ),
        Property("DatasourceObjectIds", ArrayType(StringType)),
        Property("Datasources",  
            ArrayType(
                ObjectType(
                    Property("ConnectionDetails", StringType),
                    Property("DatasourceType", StringType),
                )
            )
        ),
        Property("DatasourceType", StringType),
        Property(
            "DeploymentPipelineAccesses",
            ArrayType(
                ObjectType(
                    Property("GroupObjectId", StringType),
                    Property("RolePermissions", StringType),
                    Property("UserId", IntegerType),
                    Property("UserObjectId", StringType),
                )
            )
        ),
        Property("DeploymentPipelineDisplayName", StringType),
        Property("DeploymentPipelineId", IntegerType),
        Property("DeploymentPipelineObjectId", StringType),
        Property("DeploymentPipelineStageOrder", IntegerType),
        # Property("DetectCustomizationsRequest", ObjectType(),
        Property("DistributionMethod", StringType),
        Property("EmbedTokenId", StringType),
        Property("EndPoint", StringType),
        Property("ExcludePersonalWorkspaces", BooleanType),
        Property("Experience", StringType),
        Property(
            "ExportedArtifactInfo",
            ObjectType(
                Property("ArtifactId", IntegerType),
                Property("ArtifactType", StringType),
                Property("ExportType", StringType),
            )
        ),
        Property("ExportEventActivityTypeParameter", StringType),
        Property("ExportEventEndDateTimeParameter", DateTimeType),
        Property("ExportEventStartDateTimeParameter", DateTimeType),
        Property("ExternalSubscribeeInformation", StringType),
        Property(
           "ExternalSubscribeeInformation",
            ArrayType(
                ObjectType(
                    Property("RecipientEmail", StringType),
                )
            )
        ),
        Property(
            "FolderAccessRequests",
            ArrayType(
                ObjectType(
                    Property("GroupId", IntegerType),
                    Property("GroupObjectId", StringType),
                    Property("RolePermissions", StringType),
                    Property("UserId", IntegerType),
                    Property("UserObjectId", StringType),
                )
            )
        ),
        Property("FolderDisplayName", StringType),
        Property("FolderObjectId", StringType),
        Property("GatewayClusterId", StringType),
        Property(
            "GatewayClusters",
            ArrayType(
                ObjectType(
                    Property("id", StringType),
                    Property("memberGatewaysIds", ArrayType(StringType)),
                    Property(
                        "permissions",
                        ArrayType(
                            ObjectType(
                                Property("allowedDataSources", ArrayType(StringType)),
                                Property("id", StringType),
                                Property("principalType", StringType),
                                Property("role", StringType),
                            )
                        )
                    ),
                    Property("type", StringType),
                )
            )
        ),
        Property(
            "GatewayClusterDatasources",
            ArrayType(
                ObjectType(
                    Property("clusterId", StringType),
                    Property(
                        "credentialDetails",
                        ObjectType(
                            Property("credentialType", StringType),
                            Property("encryptedConnection", StringType),
                            Property("encryptionAlgorithm", StringType),
                            Property("isCredentialEncrypted", BooleanType),
                            Property("privacyLevel", StringType),
                            Property("skipTestConnection", BooleanType),
                            Property("useCustomOAuthApp", BooleanType),
                        )
                    ),
                    Property("credentialType", StringType),
                    Property("datasourceName", StringType),
                    Property("datasourceType", StringType),
                    Property("gatewayClusterName", StringType),
                    Property("id", StringType),
                    Property(
                        "users",
                        ArrayType(
                            ObjectType(
                                Property("identifier", StringType),
                            )
                        )
                    ),
                )
            )
        ),
        Property("GatewayClustersObjectIds", ArrayType(StringType)),
        Property("GatewayId", StringType),
        Property("GatewayMemberId", StringType),
        Property("GatewayName", StringType),
        Property("GatewayStatus", StringType),
        Property("GatewayType", StringType),
        Property(
            "GenerateScreenshotInformation",
            ObjectType(
                Property("ExportFormat", StringType),
                Property("ExportType", IntegerType),
                Property("ExportUrl", StringType),
                Property("ScreenshotEngineType", IntegerType),
            )
        ),
        Property(
            "GitIntegrationRequest",
            ObjectType(
                Property(
                    "AddedArtifacts",
                    ArrayType(
                        ObjectType(
                            Property("LogicalId", StringType),
                            Property("ObjectId", StringType),
                        )
                    )
                ),
                Property("BranchName", StringType),
                Property(
                    "DeletedArtifacts",
                    ArrayType(
                        ObjectType(
                            Property("LogicalId", StringType),
                            Property("ObjectId", StringType),
                        )
                    )
                ),
                Property("FromCommitId", StringType),
                Property(
                    "ModifiedArtifacts",
                    ArrayType(
                        ObjectType(
                            Property("LogicalId", StringType),
                            Property("ObjectId", StringType),
                        )
                    )
                ),
                Property("OrganizationName", StringType),
                Property("ProjectName", StringType),
                Property("RepositoryName", StringType),
                Property("RootDirectory", StringType),
                Property("ToCommitId", StringType),
                Property("WorkspaceId", StringType),
            )
        ),
        Property("HasFullReportAttachment", BooleanType),
        Property("ImportDisplayName", StringType),
        Property("ImportId", StringType),
        Property("ImportSource", StringType),
        Property("ImportType", StringType),
        Property("IncludeExpressions", BooleanType),
        Property("IncludeSubartifacts", BooleanType),
        Property("InstallTeamsAnalyticsInformation",
            ObjectType(
                Property("ModelId", StringType),
                Property("TenantId", StringType),
                Property("UserId", StringType),
            )
        ),
        Property("IsSuccess", BooleanType),
        Property("IsTemplateAppFromMarketplace", BooleanType),
        Property("IsTenantAdminApi", BooleanType),
        Property("IsUpdateAppActivity", BooleanType),
        Property("ItemId", StringType),
        Property("ItemName", StringType),
        Property("LastRefreshTime", StringType),
        Property("Lineage", BooleanType),
        Property("MembershipInformation", 
            ArrayType(
                ObjectType(
                    Property("MemberEmail", StringType),
                )
            )
        ),
        Property("MentionedUsersInformation", StringType),
        Property("ModelId", StringType),
        Property("ModelsSnapshots", ArrayType(IntegerType)),
        Property(
            "ModelSettings",
            ObjectType(
                    Property("DirectLakeAutoSync", BooleanType),
                )
        ),
        Property("Monikers", ArrayType(StringType)),
        Property("ObjectDisplayName", StringType),
        Property("ObjectId", StringType),
        Property("ObjectType", StringType),
        Property("Operation", StringType),
        Property("OrganizationId", StringType),
        Property(
            "OrgAppPermission",
            ObjectType(
                Property("permissions", StringType),
                Property("recipients", StringType),
            )
        ),
        Property("OriginalOwner", StringType),
        Property("PackageId", IntegerType),
        Property(
            "PaginatedReportDataSources",
            ArrayType(
                ObjectType(
                    Property("connectionString", StringType),
                    Property("credentialRetrievalType", StringType),
                    Property("dMMoniker", StringType),
                    Property("name", StringType),
                    Property("provider", StringType),
                )
            )
        ),
        Property(
            "PinReportToTabInformation", 
            ObjectType(
                Property("ChannelId", StringType),
                Property("ChannelName", StringType),
                Property("DatasetId", StringType),
                Property("DatasetName", StringType),
                Property("ReportId", StringType),
                Property("ReportName", StringType),
                Property("TabName", StringType),
                Property("TeamId", StringType),
                Property("TeamName", StringType),
                Property("TeamsAppId", StringType),
                Property("UserId", StringType),
            )
        ),
        Property("PowerPlatformEnvironmentId", StringType),
        Property(
            "PowerPlatformSolutionRequest",
            ObjectType(
                Property("DatasetIds", ArrayType(StringType)),
                Property("EnvironmentId", StringType),
                Property("FolderIds", ArrayType(StringType)),
                Property("ReportIds", ArrayType(StringType)),
            )
        ),
        Property("RecordType", IntegerType),
        Property("RefreshEnforcementPolicy", IntegerType),
        Property("RefreshType", StringType),
        Property("ReportCertificationStage", StringType),
        Property("ReportId", StringType),
        Property("ReportName", StringType),
        Property("ReportType", StringType),
        Property("RequestId", StringType),
        Property("ResultStatus", StringType),
        Property("RequiredWorkspaces", ArrayType(StringType)),
        Property(
            "Schedules",
            ObjectType(
                Property("Days", ArrayType(StringType)),
                Property("RefreshFrequency", StringType),
                Property("Time", ArrayType(StringType)),
                Property("TimeZone", StringType),
            )
        ),
        Property("SensitivityLabelId", StringType),
        Property("ShareLinkId", StringType),
        Property("ShareWithCurrentFilter", BooleanType),
        Property("SharingAction", StringType),
        Property(
            "SharingInformation",
            ArrayType(
                ObjectType(
                    Property("ObjectId", StringType),
                    Property("RecipientEmail", StringType),
                    Property("RecipientName", StringType),
                    Property("ResharePermission", StringType),
                    Property("TenantObjectId", StringType),
                    Property("UserPrincipalName", StringType),
                )
            )
        ),
        Property("SharingScope", StringType),
        Property("SingleSignOnType", StringType),
        Property("SubfolderId", IntegerType),
        Property("SubfolderName", StringType),
        Property("SubfolderObjectId", StringType),
        Property(
            "SubscriptionDetails",
            ObjectType(
                Property("attachmentType", StringType),
                Property("isOnDemand", BooleanType),
                Property("subject", StringType),
                Property("subscriptionObjectId", StringType),
                Property("title", StringType),
            )
        ),
        Property(
            "SubscribeeInformation",
            ArrayType(
                ObjectType(
                    Property("ObjectId", StringType),
                    Property("RecipientEmail", StringType),
                    Property("RecipientName", StringType),
                )
            )
        ),
        Property(
            "SubscriptionSchedule",
            ObjectType(
                Property("DaysOfTheMonth",StringType),
                Property("EndDate", DateTimeType),
                Property("StartDate", DateTimeType),
                Property(
                    "Time",
                    ArrayType(StringType)
                ),
                Property("TimeZone", StringType),
                Property("Type", StringType),
                Property(
                    "WeekDays",
                    ArrayType(StringType)
                ),
            )
        ),
        Property("SwitchState", StringType),
        Property("TableName", StringType),
        Property("TakingOverOwner", StringType),
        Property("TargetWorkspaceId", StringType),
        Property("TemplateAppFolderObjectId", StringType),
        Property("TemplateAppIsInstalledWithAutomation", BooleanType),
        Property("TemplateAppObjectId", StringType),
        Property("TemplateAppOwnerTenantObjectId", StringType),
        Property("TemplateAppVersion", StringType),
        Property("TemplatePackageName", StringType),
        Property("TileText", StringType),
        Property(
            "UpdateFeaturedTables",
            ArrayType(
                ObjectType(
                    Property("State", StringType),
                    Property("TableName", StringType),
                )
            )
        ),
        Property("UserAgent", StringType),
        Property("UserId", StringType),
        Property(
            "UserInformation",
            ObjectType(
                Property(
                    "UsersAdded",
                    ArrayType(StringType)
                ),
                Property(
                    "UsersRemoved",
                    ArrayType(StringType)
                ),
            )
        ),
        Property("UserKey", StringType),
        Property("UserType", IntegerType),
        Property("Workload", StringType),
        Property(
            "WorkspaceAccessList", 
            ArrayType(
                ObjectType(
                    Property(
                        "UserAccessList",
                        ArrayType(
                            ObjectType(
                                Property("GroupUserAccessRight", StringType),
                                Property("Identifier", StringType),
                                Property("PrincipalType", StringType),
                                Property("UserEmailAddress", StringType),
                            )
                        ),
                    ),
                    Property("WorkspaceId", StringType),
                )
            )
        ),
        Property("WorkspaceId", StringType),
        Property("WorkSpaceName", StringType),
        Property("WorkspacesModifiedSince", StringType),
        Property("WorkspacesSemicolonDelimitedList", StringType),
    ).to_dict()

class AppsStream(TapPowerBIMetadataStream):
    """Returns a list of apps in the organization.
    Docs: https://learn.microsoft.com/en-us/rest/api/power-bi/admin/apps-get-apps-as-admin
    Requires: The query parameter $top is required.
    """
    name = "Apps"
    path = "/admin/apps"
    primary_keys = ["id"]
    top_required = True
    schema = PropertiesList(
        Property("id", StringType),
        Property("description", StringType),
        Property("lastUpdate", StringType),
        Property("name", StringType),
        Property("publishedBy", StringType),
        # users  is empty value in API Docs
        # Property("users", ArrayType(StringType)),
        Property("workspaceId", StringType),
    ).to_dict()

class DatasetStream(TapPowerBIMetadataStream):
    """ Returns a list of datasets for the organization.
    Docs: https://learn.microsoft.com/en-us/rest/api/power-bi/admin/datasets-get-datasets-as-admin
    """
    name = "Datasets"
    path = "/admin/datasets"
    primary_keys = ["id"]
    # top_required = True
    # skip_required = True
    schema = PropertiesList(
        Property("id", StringType),
        Property("addRowsAPIEnabled", BooleanType),
        Property("configuredBy", StringType),
        Property("contentProviderType", StringType),
        Property("createdDate", StringType),
        Property("createReportEmbedURL", StringType),
        Property("description", StringType),
        Property("isEffectiveIdentityRequired", BooleanType),
        Property("isEffectiveIdentityRolesRequired", BooleanType),
        Property("isInPlaceSharingEnabled", BooleanType),
        Property("isOnPremGatewayRequired", BooleanType),
        Property("isRefreshable", BooleanType),
        Property("name", StringType),
        Property("qnaEmbedURL", StringType),
        Property(
            "queryScaleOutSettings", 
            ObjectType(
                Property("autoSyncReadOnlyReplicas", BooleanType),
                Property("maxReadOnlyReplicas", IntegerType),
            )
        ),
        Property("targetStorageMode", StringType),
        Property(
            "upstreamDataflows",
            ObjectType(
                Property("groupID", StringType),
                Property("targetDataflowId", StringType)
            )
        ),
        Property(
            "upstreamDatasets",
            ArrayType(
                ObjectType(
                    Property("DatasetId", StringType),
                    Property("DatasetName", StringType),
                )
            )
        ),
        # users is empty and will be removed in a future release
        # Property("users", ArrayType(StringType)),
        Property("webUrl", StringType),
        Property("workspaceId", StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context:Optional[dict]) -> dict:
        """ Return a context dictionary for child streams."""
        return {
            "datasetId": record["id"]
        }
    
# Child Stream DataSources must come after Parent Stream Datasets
class DataSourceStream(TapPowerBIMetadataStream):
    """ Returns a list of datasets for the organization.
    Docs: https://learn.microsoft.com/en-us/rest/api/power-bi/admin/datasets-get-datasets-as-admin
    """
    name = "DataSources"
    path = "/admin/datasets/{datasetId}/datasources"
    primary_keys = ["datasetId","datasourceId"]
    parent_stream_type = DatasetStream
    # top_required = True
    # skip_required = True
    schema = PropertiesList(
        Property(
            "connectionDetails", 
            ObjectType(
                Property("account", StringType),
                Property("classInfo", StringType),
                Property("connectionString", StringType),
                Property("database", StringType),
                Property("domain", StringType),
                Property("emailAddress", StringType),
                Property("kind", StringType),
                Property("loginServer", StringType),
                Property("path", StringType),
                Property("server", StringType),
                Property("url", StringType),
            )
        ),
        Property("connectionString", StringType),
        Property("datasetId", StringType),
        Property("datasourceId", StringType),
        Property("datasourceType", StringType),
        Property("gatewayId", StringType),
        Property("name", StringType),
    ).to_dict()

class GroupsStream(TapPowerBIMetadataStream):
    """ Returns a list of workspaces for the organization.
    Docs: https://learn.microsoft.com/en-us/rest/api/power-bi/admin/groups-get-groups-as-admin#admindashboard

    Requires: The query parameter $top is required.
              Uses $expand to inline expand certain data types. 
    """
    name = "Groups"
    path = "/admin/groups"
    primary_keys = ["id"]
    top_required = True
    skip_required = True
    uri_parameters = {'$expand':'users,reports,dashboards,datasets,dataflows,workbooks'}
    schema = PropertiesList(
        Property("id", StringType),
        Property("capacityId", StringType),
        Property("capacityMigrationStatus", StringType),
        Property("dataflowStorageId", StringType),
        Property("defaultDatasetStorageFormat", StringType),
        Property("description", StringType),
        Property("hasWorkspaceLevelSettings ", BooleanType),
        Property("isOnDedicatedCapacity", BooleanType),
        Property("isReadOnly", BooleanType),
        Property("logAnalyticsWorkspace", StringType),
        Property("name", StringType),
        Property("pipelineId", StringType),
        Property("state", StringType),
        Property("type", StringType),
        # users is empty value in API Docs
        # Property("users", ArrayType(StringType)),
        Property(
            "dashboards",
            ArrayType(
                ObjectType(
                    Property("appId", StringType),
                    Property("displayName", StringType),
                    Property("embedUrl", StringType),
                    Property("id", StringType),
                    Property("isReadOnly", BooleanType),
                    # subscriptions is empty value in API Docs
                    # Property("subscriptions", ArrayType(StringType)),
                    Property(
                        "tiles", 
                        ArrayType(
                            ObjectType(
                                Property("colSpan", IntegerType),
                                Property("datasetId", StringType),
                                Property("embedData", StringType),
                                Property("embedUrl", StringType),
                                Property("id", StringType),
                                Property("reportId", StringType),
                                Property("rowSpan", IntegerType),
                                Property("title", StringType),
                            )
                        )
                    ),
                    # users is empty value in API Docs
                    # Property("users", ArrayType(StringType)),
                    Property("webUrl", StringType),
                    Property("workspaceId", StringType),
                )
            )
        ),
        Property(
            "dataflows",
            ArrayType(
                ObjectType(
                    Property("configuredBy", StringType),
                    Property("description", StringType),
                    Property("generation", IntegerType),
                    Property("modelUrl", StringType),
                    Property("modifiedBy", StringType),
                    Property("modifiedDateTime", StringType),
                    Property("name", StringType),
                    Property("objectId", StringType),
                    # users is empty value in API Docs
                    # Property("users", ArrayType(StringType)),
                    Property("workspaceId", StringType),
                )
            )
        ),
        Property(
            "datasets",
            ArrayType(
                ObjectType(
                    Property("contentProviderType", StringType),
                    Property("CreateReportEmbedUrl", StringType),
                    Property("createdDate", StringType),
                    Property("Encryption", StringType),
                    Property("isEffectiveIdentityRequired", BooleanType),
                    Property("isEffectiveIdentityRolesRequired", BooleanType),
                    Property("isInPlaceSharingEnabled", BooleanType),
                    Property("IsOnPremGatewayRequired", BooleanType),
                    Property("isRefreshable", BooleanType),
                    Property("QnaEmbedUrl", StringType),
                    Property("addRowsAPIEnabled", BooleanType),
                    Property("configuredBy", StringType),
                    Property("description", StringType),
                    Property("id", StringType),
                    Property("name", StringType),
                    Property("queryScaleOutSettings", StringType),
                    Property("targetStorageMode", StringType),
                    Property("upstreamDatasets", ArrayType(StringType)),
                    Property(
                        "upstreamDataflows",
                        ArrayType(
                            ObjectType(
                                Property("groupId", StringType),
                                Property("targetDataflowId", StringType),
                            )
                        )
                    ),
                    # users is empty value in API Docs
                    # Property("users", ArrayType(StringType)),
                    Property("webUrl", StringType),
                    Property("workspaceId", StringType),
                )
            )
        ),
        Property(
            "reports",
            ArrayType(
                ObjectType(
                    Property("appId", StringType),
                    Property("createdBy", StringType),
                    Property("createdDateTime", StringType),
                    Property("datasetId", StringType),
                    Property("description", StringType),
                    Property("embedUrl", StringType),
                    Property("id", StringType),
                    Property("modifiedBy", StringType),
                    Property("modifiedDateTime", StringType),
                    Property("name", StringType),
                    Property("reportType", StringType),
                    # sections is empty
                    # Property("sections", ArrayType(StringType)),
                    # subscriptions is empty value in API Docs
                    # Property("subscriptions", ArrayType(StringType)),
                    # users is empty value in API Docs
                    # Property("users", ArrayType(StringType)),
                    Property("webUrl", StringType),
                    Property("workspaceId", StringType),
                )
            )
        ),
        Property(
            "users",
            ArrayType(
                ObjectType(
                    Property("displayName", StringType),
                    Property("emailAddress", StringType),
                    Property("graphId", StringType),
                    Property("groupUserAccessRight", StringType),
                    Property("identifier", StringType),
                    Property("principalType", StringType),
                    Property("profile", StringType),
                    Property("userType", StringType),
                )
            )
        ),
        Property(
            "workbooks",
            ArrayType(
                ObjectType(
                    Property("name", StringType),
                    Property("datasetId", StringType),
                )
            )
        )
    ).to_dict()

class ReportsStream(TapPowerBIMetadataStream):
    """ Returns a list of reports for the organization.
    Docs: https://learn.microsoft.com/en-us/rest/api/power-bi/admin/reports-get-reports-as-admin
    """
    name = "Reports"
    path = "/admin/reports"
    primary_keys = ["id"]
    schema = PropertiesList(
        Property("id", StringType),
        Property("appId", StringType),
        Property("createdBy", StringType),
        Property("createdDateTime", StringType),
        Property("datasetId", StringType),
        Property("description", StringType),
        Property("embedUrl", StringType),
        Property(
            "endorsementDetails",
            ObjectType(
                Property("certifiedBy", StringType),
                Property("endorsement", StringType),
            )
        ),
        Property("modifiedBy", StringType),
        Property("modifiedDateTime", StringType),
        Property("name", StringType),
        Property("originalReportObjectId", StringType),
        Property("reportType", StringType),
        # sections is empty
        # Property("sections", ArrayType(StringType)),
        Property(
            "sensitivityLabel",
            ObjectType(
                Property("labelId", StringType),
            )
        ),
        # subscriptions is empty value in API Docs
        # Property("subscriptions", ArrayType(StringType)),
        # users  is empty value in API Docs
        # Property("users", ArrayType(StringType)),
        Property("webUrl", StringType),
        Property("workspaceId", StringType),
    ).to_dict()