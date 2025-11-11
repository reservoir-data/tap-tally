"""Stream type classes for tap-tally."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any, override

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.pagination import BasePageNumberPaginator

from tap_tally.client import TallyStream

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class _OrganizationStream(TallyStream):
    """Organization stream."""

    @override
    @cached_property
    def partitions(self) -> list[dict] | None:
        org_ids: list[str] = self.config.get("organization_ids", [])
        if not org_ids:
            user_info_request = requests.Request(method="GET", url=f"{self.url_base}/users/me")
            prepared_request = user_info_request.prepare()
            response = self._request(prepared_request, None)
            response.raise_for_status()
            me = response.json()
            org_ids = [me["organizationId"]]

        return [{"organizationId": org_id} for org_id in org_ids]


class UsersStream(_OrganizationStream):
    """Users stream."""

    name = "users"
    path = "/organizations/{organizationId}/users"
    primary_keys = ("id",)
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("firstName", th.StringType),
        th.Property("lastName", th.StringType),
        th.Property("fullName", th.StringType),
        th.Property("email", th.EmailType),
        th.Property("avatarUrl", th.URIType),
        th.Property("organizationId", th.StringType),
        th.Property("isBlocked", th.BooleanType),
        th.Property("isDeleted", th.BooleanType),
        th.Property("timezone", th.StringType),
        th.Property("hasTwoFactorEnabled", th.BooleanType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("subscriptionPlan", th.StringType),
        th.Property("ssoIsConnectedWithGoogle", th.BooleanType),
        th.Property("ssoIsConnectedWithApple", th.BooleanType),
        th.Property("hasPasswordSet", th.BooleanType),
        th.Property("authenticationMethodsCount", th.IntegerType),
        th.Property("emailDomain", th.StringType),
    ).to_dict()


class InvitesStream(_OrganizationStream):
    """Invites stream."""

    name = "invites"
    path = "/organizations/{organizationId}/invites"
    primary_keys = ("id",)
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("organizationId", th.StringType),
        th.Property("email", th.EmailType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()


class FormsStream(_OrganizationStream):
    """Forms stream."""

    PAGE_SIZE = 500

    records_jsonpath = "$.items[*]"

    name = "forms"
    path = "/forms"
    primary_keys = ("id",)
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("workspaceId", th.StringType),
        th.Property("status", th.StringType),
        th.Property("numberOfSubmissions", th.IntegerType),
        th.Property("isClosed", th.BooleanType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property(
            "payments",
            th.PropertiesList(
                th.Property("amount", th.NumberType),
                th.Property("currency", th.StringType),
            ),
        ),
    ).to_dict()

    @override
    def get_new_paginator(self) -> BasePageNumberPaginator:
        return BasePageNumberPaginator(start_value=1)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = super().get_url_params(context, next_page_token)  # type: ignore[assignment]
        params["limit"] = self.PAGE_SIZE
        if next_page_token is not None:
            params["page"] = next_page_token
        return params


class QuestionsStream(TallyStream):
    """Questions stream."""

    parent_stream_type = FormsStream
    records_jsonpath = "$.questions[*]"

    name = "questions"
    path = "/forms/{formId}/questions"
    primary_keys = ("id",)
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("title", th.StringType),
        th.Property("isTitleModifiedByUser", th.BooleanType),
        th.Property("formId", th.StringType),
        th.Property("isDeleted", th.BooleanType),
        th.Property("numberOfResponses", th.IntegerType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property(
            "fields",
            th.PropertiesList(
                th.Property("uuid", th.StringType),
                th.Property("type", th.StringType),
                th.Property("blockGroupUuid", th.StringType),
                th.Property("title", th.IntegerType),
            ),
        ),
        th.Property("hasResponses", th.BooleanType),
    ).to_dict()


class SubmissionsStream(TallyStream):
    """Submissions stream."""

    SUBMISSION_FILTER = "all"

    parent_stream_type = FormsStream
    records_jsonpath = "$.submissions[*]"

    name = "submissions"
    path = "/forms/{formId}/submissions"
    primary_keys = ("id",)
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("formId", th.StringType),
        th.Property("isCompleted", th.BooleanType),
        th.Property("submittedAt", th.DateTimeType),
        th.Property(
            "responses",
            th.ArrayType(
                th.ObjectType(
                    th.Property("questionId", th.StringType),
                    th.Property("value", th.AnyType),
                )
            ),
        ),
    ).to_dict()

    @override
    def get_new_paginator(self) -> BasePageNumberPaginator:
        return BasePageNumberPaginator(start_value=1)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = super().get_url_params(context, next_page_token)  # type: ignore[assignment]
        params["filter"] = self.SUBMISSION_FILTER
        if next_page_token is not None:
            params["page"] = next_page_token
        return params


class WorkspacesStream(TallyStream):
    """Workspaces stream."""

    records_jsonpath = "$.items[*]"

    name = "workspaces"
    path = "/workspaces"
    primary_keys = ("id",)
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property(
            "members",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("firstName", th.StringType),
                    th.Property("lastName", th.StringType),
                    th.Property("fullName", th.StringType),
                    th.Property("email", th.EmailType),
                    th.Property("avatarUrl", th.URIType),
                    th.Property("organizationId", th.StringType),
                    th.Property("hasTwoFactorEnabled", th.BooleanType),
                    th.Property("createdAt", th.DateTimeType),
                    th.Property("updatedAt", th.DateTimeType),
                    th.Property("subscriptionPlan", th.StringType),
                    th.Property("ssoIsConnectedWithGoogle", th.BooleanType),
                    th.Property("ssoIsConnectedWithApple", th.BooleanType),
                    th.Property("hasPasswordSet", th.BooleanType),
                    th.Property("authenticationMethodsCount", th.IntegerType),
                    th.Property("emailDomain", th.StringType),
                )
            ),
        ),
        th.Property(
            "invites",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("email", th.EmailType),
                    th.Property("workspaceIds", th.ArrayType(th.StringType())),
                ),
            ),
        ),
        th.Property("createdByUserId", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()

    @override
    def get_new_paginator(self) -> BasePageNumberPaginator:
        return BasePageNumberPaginator(start_value=1)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = super().get_url_params(context, next_page_token)  # type: ignore[assignment]
        if next_page_token is not None:
            params["page"] = next_page_token
        return params


class WebhooksStream(TallyStream):
    """Webhooks stream."""

    PAGE_SIZE = 100

    records_jsonpath = "$.webhooks[*]"

    name = "webhooks"
    path = "/webhooks"
    primary_keys = ("id",)
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("formId", th.StringType),
        th.Property("url", th.URIType),
        th.Property("signingSecret", th.StringType),
        th.Property(
            "httpHeaders",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("value", th.StringType),
                )
            ),
        ),
        th.Property("eventTypes", th.ArrayType(th.StringType())),
        th.Property("externalSubscriber", th.StringType),
        th.Property("isEnabled", th.BooleanType),
        th.Property("lastSyncedAt", th.DateTimeType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()

    @override
    def get_new_paginator(self) -> BasePageNumberPaginator:
        return BasePageNumberPaginator(start_value=1)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = super().get_url_params(context, next_page_token)  # type: ignore[assignment]
        params["limit"] = self.PAGE_SIZE
        if next_page_token is not None:
            params["page"] = next_page_token
        return params
