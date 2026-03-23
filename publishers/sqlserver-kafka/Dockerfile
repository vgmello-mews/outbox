FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/Outbox.Core/Outbox.Core.csproj src/Outbox.Core/
COPY src/Outbox.SqlServer/Outbox.SqlServer.csproj src/Outbox.SqlServer/
COPY src/Outbox.Kafka/Outbox.Kafka.csproj src/Outbox.Kafka/
COPY publishers/sqlserver-kafka/Outbox.Publisher.SqlServer.Kafka.csproj publishers/sqlserver-kafka/
RUN dotnet restore publishers/sqlserver-kafka/Outbox.Publisher.SqlServer.Kafka.csproj
COPY src/ src/
COPY publishers/sqlserver-kafka/ publishers/sqlserver-kafka/
RUN dotnet publish publishers/sqlserver-kafka/Outbox.Publisher.SqlServer.Kafka.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "Outbox.Publisher.SqlServer.Kafka.dll"]
