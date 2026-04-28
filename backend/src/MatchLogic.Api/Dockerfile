# syntax=docker/dockerfile:1.7
# MatchLogic Profiling SaaS — production container for the .NET 8 API.
# Multi-stage: SDK image restores + publishes; alpine runtime image runs the
# result. Build context is backend/ (the directory containing this csproj
# tree and Directory.Packages.props).
#
# Build locally:
#   docker build -f src/MatchLogic.Api/Dockerfile -t matchlogic-api:dev .
# Run locally:
#   docker run --rm -p 8080:8080 \
#     -e Cognito__Region=us-east-1 \
#     -e Cognito__UserPoolId=... \
#     -e Cognito__ClientId=... \
#     -e MongoDB__ConnectionString=... \
#     matchlogic-api:dev

ARG DOTNET_VERSION=8.0

# ---- build stage ---------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:${DOTNET_VERSION}-alpine AS build
WORKDIR /src

# Restore in its own layer so package changes don't bust the source-copy cache.
COPY Directory.Packages.props ./
COPY src/MatchLogic.Api/MatchLogic.Api.csproj                       src/MatchLogic.Api/
COPY src/MatchLogic.Application/MatchLogic.Application.csproj       src/MatchLogic.Application/
COPY src/MatchLogic.Domain/MatchLogic.Domain.csproj                 src/MatchLogic.Domain/
COPY src/MatchLogic.Infrastructure/MatchLogic.Infrastructure.csproj src/MatchLogic.Infrastructure/
RUN --mount=type=cache,target=/root/.nuget/packages \
    dotnet restore src/MatchLogic.Api/MatchLogic.Api.csproj

COPY . .
RUN --mount=type=cache,target=/root/.nuget/packages \
    dotnet publish src/MatchLogic.Api/MatchLogic.Api.csproj \
        -c Release \
        -o /app/publish \
        --no-restore \
        /p:UseAppHost=false

# ---- runtime stage -------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:${DOTNET_VERSION}-alpine AS runtime
WORKDIR /app

# icu-libs: globalization (Excel/CSV parsing). curl: HEALTHCHECK probe.
# The "app" non-root user is built into mcr.microsoft.com/dotnet/aspnet:8.0
# already, so we just create the writable dirs and chown them to it.
RUN apk add --no-cache icu-libs curl \
 && mkdir -p /app/Uploads /app/logs /app/temp /app/data \
 && chown -R app:app /app

ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false \
    DOTNET_RUNNING_IN_CONTAINER=true \
    ASPNETCORE_ENVIRONMENT=Production \
    ASPNETCORE_URLS=http://+:8080

USER app

COPY --chown=app:app --from=build /app/publish ./

EXPOSE 8080

# ALB target group health check will probe /api/HealthCheck (returns 200 "OK").
# Keeping a docker-level HEALTHCHECK lets local `docker run` / Compose see status.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:8080/api/HealthCheck/ || exit 1

ENTRYPOINT ["dotnet", "MatchLogic.Api.dll"]
