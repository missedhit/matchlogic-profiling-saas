FROM mcr.microsoft.com/dotnet/aspnet:8.0-alpine AS base
WORKDIR /app
EXPOSE 5000

# Install cultures for globalization support
RUN apk add --no-cache icu-libs

# Disable the invariant mode (set in base image)
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false
ENV ASPNETCORE_URLS=http://+:5000
ENV ASPNETCORE_ENVIRONMENT=Production
ENV DOTNET_RUNNING_IN_CONTAINER=true

# Create application directories
RUN mkdir -p /app/Uploads /app/logs /app/temp /app/data

# Copy pre-built published output
COPY bin/Release/net8.0/publish/ .

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget -q --spider http://localhost:5000/api/HealthCheck/echo?text=health || exit 1

ENTRYPOINT ["dotnet", "DataLadder.Api.dll"]
