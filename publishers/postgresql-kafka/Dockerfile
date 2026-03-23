FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/Outbox.Core/Outbox.Core.csproj src/Outbox.Core/
COPY src/Outbox.PostgreSQL/Outbox.PostgreSQL.csproj src/Outbox.PostgreSQL/
COPY src/Outbox.Kafka/Outbox.Kafka.csproj src/Outbox.Kafka/
COPY publishers/postgresql-kafka/Outbox.Publisher.PostgreSQL.Kafka.csproj publishers/postgresql-kafka/
RUN dotnet restore publishers/postgresql-kafka/Outbox.Publisher.PostgreSQL.Kafka.csproj
COPY src/ src/
COPY publishers/postgresql-kafka/ publishers/postgresql-kafka/
RUN dotnet publish publishers/postgresql-kafka/Outbox.Publisher.PostgreSQL.Kafka.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "Outbox.Publisher.PostgreSQL.Kafka.dll"]
