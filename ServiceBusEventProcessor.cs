using System.Net.Http.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace BgWorkerFA
{
    public class ServiceBusEventProcessor
    {
        private static readonly HashSet<string> ProcessedEventIds = new HashSet<string>();
        private static readonly HttpClient HttpClient = new HttpClient();

        [Function(nameof(ServiceBusEventProcessor))]
        public async Task Run(
            [ServiceBusTrigger("InputQueue", Connection = "ConnectionString")]
            ServiceBusReceivedMessage message,
            ILogger _logger)
        {
            var eventMessage = JsonConvert.DeserializeObject<EventMessage>(System.Text.Encoding.UTF8.GetString(message.Body));

            _logger.LogInformation($"Received event: {eventMessage.EventId}");

            if (ProcessedEventIds.Contains(eventMessage.EventId))
            {
                _logger.LogInformation($"Duplicate event found. Skipping event: {eventMessage.EventId}");
                return; 
            }
            ProcessedEventIds.Add(eventMessage.EventId);

            var apiUrl = "https://eventprocessor.azure-api.net/api/api/events/process";

            var response = await HttpClient.PostAsJsonAsync(apiUrl, eventMessage);

            if (response.IsSuccessStatusCode)
            {
                _logger.LogInformation($"Event with EventId: {eventMessage.EventId} successfully sent to the EventProcessorAPI.");
            }
            else
            {
                _logger.LogError($"Failed to send event with EventId: {eventMessage.EventId}. Status code: {response.StatusCode}");
            }
        }
    }
}
