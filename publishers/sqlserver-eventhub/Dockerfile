FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/Outbox.Core/Outbox.Core.csproj src/Outbox.Core/
COPY src/Outbox.SqlServer/Outbox.SqlServer.csproj src/Outbox.SqlServer/
COPY src/Outbox.EventHub/Outbox.EventHub.csproj src/Outbox.EventHub/
COPY publishers/sqlserver-eventhub/Outbox.Publisher.SqlServer.EventHub.csproj publishers/sqlserver-eventhub/
RUN dotnet restore publishers/sqlserver-eventhub/Outbox.Publisher.SqlServer.EventHub.csproj
COPY src/ src/
COPY publishers/sqlserver-eventhub/ publishers/sqlserver-eventhub/
RUN dotnet publish publishers/sqlserver-eventhub/Outbox.Publisher.SqlServer.EventHub.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "Outbox.Publisher.SqlServer.EventHub.dll"]
