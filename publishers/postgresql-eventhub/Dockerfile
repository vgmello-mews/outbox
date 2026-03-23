FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/Outbox.Core/Outbox.Core.csproj src/Outbox.Core/
COPY src/Outbox.PostgreSQL/Outbox.PostgreSQL.csproj src/Outbox.PostgreSQL/
COPY src/Outbox.EventHub/Outbox.EventHub.csproj src/Outbox.EventHub/
COPY publishers/postgresql-eventhub/Outbox.Publisher.PostgreSQL.EventHub.csproj publishers/postgresql-eventhub/
RUN dotnet restore publishers/postgresql-eventhub/Outbox.Publisher.PostgreSQL.EventHub.csproj
COPY src/ src/
COPY publishers/postgresql-eventhub/ publishers/postgresql-eventhub/
RUN dotnet publish publishers/postgresql-eventhub/Outbox.Publisher.PostgreSQL.EventHub.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "Outbox.Publisher.PostgreSQL.EventHub.dll"]
