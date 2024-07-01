using Azure.Messaging.ServiceBus;
using Dapper;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Company.Function
{
    public class ServiceBusQueueTrigger1
    {
        private readonly ILogger<ServiceBusQueueTrigger1> _logger;
        private const string ConnectionString = "Host=c-azure-cosmos-db-psql.fhxpikxvze4hpu.postgres.cosmos.azure.com;Port=5432;Database=test-cosmos-db;Username=citus;Password=testcosmosdb1955@;SslMode=Require";

        public ServiceBusQueueTrigger1(ILogger<ServiceBusQueueTrigger1> logger)
        {
            _logger = logger;
        }

        [Function(nameof(ServiceBusQueueTrigger1))]
        public async Task Run(
            [ServiceBusTrigger("service-bus-que", Connection = "servicebus1955_SERVICEBUS")] ServiceBusReceivedMessage message,
            ServiceBusMessageActions messageActions)
        {
            _logger.LogInformation($"#Message ID: {message.MessageId}, " +
                                   $"Message Body: {message.Body}, " +
                                   $"Message Content-Type: {message.ContentType}, " +
                                   $"Enqueued Time: {message.EnqueuedTime}, " +
                                   $"Sequence Number: {message.SequenceNumber}");
            var registration = System.Text.Json.JsonSerializer.Deserialize<EventRegistrationDto>(message.Body.ToString());

            try
            {
                // Process the message based on the Action
                if (registration.Action == "Register")
                {
                    await RegisterEventAsync(registration.EventId, registration.UserId);
                    _logger.LogInformation($"Registered User {registration.UserId} for Event {registration.EventId}");
                }
                else if (registration.Action == "Unregister")
                {
                    await UnregisterEventAsync(registration.EventId, registration.UserId);
                    _logger.LogInformation($"Unregistered User {registration.UserId} from Event {registration.EventId}");
                }

                // Complete the message
                await messageActions.CompleteMessageAsync(message);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error processing message: {ex.Message}");
            }
        }


        private async Task RegisterEventAsync(string eventId, string userId)
        {
            using (var connection = new NpgsqlConnection(ConnectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        var eventGuid = Guid.Parse(eventId);
                        var eventQuery = "SELECT * FROM \"Events\" WHERE \"Id\" = @EventId FOR UPDATE";
                        var eventDetails = await connection.QuerySingleOrDefaultAsync(eventQuery, new { EventId = eventGuid }, transaction);

                        if (eventDetails == null)
                        {
                            throw new Exception("Event not found.");
                        }

                        if (eventDetails.RegisteredCount >= eventDetails.TotalSpots)
                        {
                            throw new Exception("No available spots.");
                        }

                        var parameters = new { EventId = eventGuid, UserId = userId, RegistrationTime = DateTime.UtcNow, Status = "Registered" };
                        var insertRegistrationQuery = "INSERT INTO \"EventRegistrations\" (\"EventId\", \"UserId\", \"RegistrationTime\", \"Status\") VALUES (@EventId, @UserId, @RegistrationTime, @Status)";

                        await connection.ExecuteAsync(insertRegistrationQuery, parameters, transaction);

                        // Increment the RegisteredCount
                        var updateEventQuery = "UPDATE \"Events\" SET \"RegisteredCount\" = \"RegisteredCount\" + 1 WHERE \"Id\" = @EventId";
                        await connection.ExecuteAsync(updateEventQuery, new { EventId = eventGuid }, transaction);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        _logger.LogError($"Error during registration: {ex.Message}");
                        throw;
                    }
                }
            }
        }


        private async Task UnregisterEventAsync(string eventId, string userId)
        {
            using (var connection = new NpgsqlConnection(ConnectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        var deleteRegistrationQuery = "DELETE FROM \"EventRegistrations\" WHERE \"EventId\" = @EventId AND \"UserId\" = @UserId AND \"Status\" = 'Registered';";
                        var result = await connection.ExecuteAsync(deleteRegistrationQuery, new { EventId = eventId, UserId = userId }, transaction);

                        var updateEventQuery = "UPDATE \"Events\" SET \"RegisteredCount\" = \"RegisteredCount\" - 1 WHERE \"Id\" = @EventId AND \"RegisteredCount\" > 0;";
                        await connection.ExecuteAsync(updateEventQuery, new { EventId = Guid.Parse(eventId) }, transaction);

                        _logger.LogInformation($"Successfully deleted registration for User {userId} from Event {eventId}. Event count decremented.");


                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        _logger.LogError($"Error during unregistration: {ex.Message}");
                        throw;
                    }
                }
            }
        }


    }

    public class EventRegistrationDto
    {
        public string EventId { get; set; }
        public string UserId { get; set; }
        public string Action { get; set; }
    }
}
