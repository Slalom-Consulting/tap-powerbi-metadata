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

API_DATE_FORMAT = "'%Y-%m-%dT%H:%M:%SZ'"

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
                    Property("RolePermissions", StringType),
                    Property("UserObjectId", StringType),
                )
            )
        ),
        Property("DeploymentPipelineDisplayName", StringType),
        Property("DeploymentPipelineId", IntegerType),
        Property("DeploymentPipelineObjectId", StringType),
        Property("DeploymentPipelineStageOrder", IntegerType),
        Property("DistributionMethod", StringType),
        Property("EndPoint", StringType),
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
                    Property("RolePermissions", StringType),
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
        Property("GatewayClustersObjectIds", ArrayType(StringType)),
        Property("GatewayId", StringType),
        Property("GatewayMemberId", StringType),
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
        Property("HasFullReportAttachment", BooleanType),
        Property("ImportDisplayName", StringType),
        Property("ImportId", StringType),
        Property("ImportSource", StringType),
        Property("ImportType", StringType),
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
        Property("ItemName", StringType),
        Property("LastRefreshTime", StringType),
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
        Property("PackageId", StringType),
        Property(
            "PaginatedReportDataSources",
            ArrayType(
                ObjectType(
                    Property("connectionString", StringType),
                    Property("credentialRetrievalType", StringType),
                    Property("", StringType),
                    Property("name", StringType),
                    Property("provider", StringType),
                )
            )
        ),
        Property("PinReportToTabInformation", 
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
        Property("RecordType", IntegerType),
        Property("RefreshEnforcementPolicy", IntegerType),
        Property("RefreshType", StringType),
        Property("ReportCertificationStage", StringType),
        Property("ReportId", StringType),
        Property("ReportName", StringType),
        Property("ReportType", StringType),
        Property("RequestId", StringType),
        Property("ResultStatus", StringType),
        Property(
            "Schedules",
            ObjectType(
                Property("Days", ArrayType(StringType)),
                Property("RefreshFrequency", StringType),
                Property("Time", ArrayType(StringType)),
                Property("TimeZone", StringType),
            )
        ),
        Property("ShareLinkId", StringType),
        Property("ShareWithCurrentFilter", BooleanType),
        Property("SharingAction", StringType),
        Property(
            "SharingInformation",
            ArrayType(
                ObjectType(
                    Property("RecipientEmail", StringType),
                    Property("ResharePermission", StringType),
                )
            )
        ),
        Property("SharingScope", StringType),
        Property("SingleSignOnType", StringType),
        Property("SwitchState", StringType),
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
        Property("workspaceId", StringType),
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
        Property("webUrl", StringType),
        Property("workspaceId", StringType),
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
        Property(
            "dashboards",
            ArrayType(
                ObjectType(
                    Property("appId", StringType),
                    Property("displayName", StringType),
                    Property("embedUrl", StringType),
                    Property("id", StringType),
                    Property("isReadOnly", BooleanType),
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
                    Property("modelUrl", StringType),
                    Property("modifiedBy", StringType),
                    Property("modifiedDateTime", StringType),
                    Property("name", StringType),
                    Property("objectId", StringType),
                    Property("workspaceId", StringType),
                )
            )
        ),
        Property(
            "datasets",
            ArrayType(
                ObjectType(
                    Property("ContentProviderType", StringType),
                    Property("CreateReportEmbedUrl", StringType),
                    Property("CreatedDate", StringType),
                    Property("Encryption", StringType),
                    Property("IsEffectiveIdentityRequired", BooleanType),
                    Property("IsEffectiveIdentityRolesRequired", BooleanType),
                    Property("IsInPlaceSharingEnabled", BooleanType),
                    Property("IsOnPremGatewayRequired", BooleanType),
                    Property("IsRefreshable", BooleanType),
                    Property("QnaEmbedUrl", StringType),
                    Property("addRowsAPIEnabled", BooleanType),
                    Property("configuredBy", StringType),
                    Property("description", StringType),
                    Property("id", StringType),
                    Property("name", StringType),
                    Property("queryScaleOutSettings", StringType),
                    Property("targetStorageMode", StringType),
                    Property(
                        "upstreamDataflows",
                        ArrayType(
                            ObjectType(
                                Property("groupId", StringType),
                                Property("targetDataflowId", StringType),
                            )
                        )
                    ),
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
        Property("webUrl", StringType),
        Property("workspaceId", StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context:Optional[dict]) -> dict:
        """ Return a context dictionary for child streams."""
        return {
            "datasetId": record["id"]
        }
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
        Property("datasourceId", StringType),
        Property("datasetId", StringType),
        Property("name", StringType),
        Property("connectionString", StringType),
        Property("gatewayId", StringType),
        Property(
            "connectionDetails", 
            ObjectType(
                Property("server", StringType),
                Property("database", StringType),
            )
        ),
    ).to_dict()